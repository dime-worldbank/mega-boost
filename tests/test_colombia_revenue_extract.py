"""Integration tests for Colombia/extract_revenue_to_csv: run the notebook end to end against
fixture inputs shaped like the real MinHacienda / FUT / CUIPO files and compare each produced
CSV to a committed golden file. All fixture values are synthetic.

Set WRITE_GOLDEN=1 to (re)generate the golden CSVs after an intentional change.
"""
import importlib.util
import os
import shutil
import unittest
from unittest import mock

import pandas as pd

from helpers import REPO_ROOT, run_script, write_workbook

NB = "Colombia/extract_revenue_to_csv.py"
NB_DIR = os.path.join(REPO_ROOT, "Colombia")
FIXTURES = os.path.join(REPO_ROOT, "tests", "fixtures", "colombia")
TMP = os.path.join(REPO_ROOT, "tests", ".fixtures_tmp", "col")
INPUT_ROOT = os.path.join(TMP, "input")
OUT_DIR = os.path.join(TMP, "out")
CENTRAL_DIR = os.path.join(INPUT_ROOT, "Colombia", "central", "revenues")
AUX_DIR = os.path.join(INPUT_ROOT, "Colombia", "auxiliary")
SUBNAT_DIR = os.path.join(INPUT_ROOT, "Colombia", "subnational", "ingresos")
WRITE_GOLDEN = os.environ.get("WRITE_GOLDEN") == "1"


def cuadro(title, year, rows, approved_hdr="Definitivo", executed_hdr="Recaudo", concepto_row=0):
    """A sheet in the MinHacienda Cuadros layout: title block, two-row header, values in
    cols 3 (approved) and 4 (executed), in miles de millones, then footer notes.
    concepto_row=1 puts "Concepto" on the second header row, as 2025's C1 does."""
    concepto = ["Concepto", None] if concepto_row == 0 else [None, "Concepto"]
    return [
        [None] * 7, [None] * 7, [None] * 7, [None] * 7,
        [title], [f"Acumulado al mes de diciembre de {year}"], ["Miles de millones de pesos"], [None] * 7,
        [concepto[0], "Aforo", None, None, executed_hdr, "Aforo menos Recaudo", "Porcentaje"],
        [concepto[1], "Inicial", "Modificación", approved_hdr, None, None, None],
        [None, "(1)", "(2)", "(3)=(1+2)", "(4)", "(5)=(3-4)", "(6)=(4/3)"],
        *[[label, a, 0, a, e, a - e, None] for label, a, e in rows],
        ["Fuente: Ministerio de Hacienda y Crédito Público.", None, None, None, None, None, None],
        ["Nota 1/: Clasificación impuestos indirectos", None, None, None, None, None, None],
    ]


def cuadros_workbook(year, extra_c2=(), **hdr):
    c2 = [
        ("Ingresos Tributarios", 151.5, 131.5),
        ("Impuestos Directos", 101.5, 91.5),
        ("Impuesto sobre la Renta y Complementarios", 100, 90),
        ("Impuesto simple", 1.5, 1.5),
        ("Impuestos Indirectos internos", 50, 40),
        ("Impuesto Sobre las Ventas 1/", 50, 40),
        *extra_c2,
        ("Ingresos No Tributarios", 2, 2),
        ("Otras Tasas, Multas y Otras Contribuciones", 2, 2),
    ]
    total_c2 = sum(a for label, a, e in c2 if label in ("Ingresos Tributarios", "Ingresos No Tributarios")) + sum(a for _, a, _ in extra_c2)
    total_c2_e = sum(e for label, a, e in c2 if label in ("Ingresos Tributarios", "Ingresos No Tributarios")) + sum(e for _, _, e in extra_c2)
    return {
        "C1 Total ingresos": cuadro("Cuadro No. 1.", year, [
            ("Ingresos del Presupuesto Nacional", 169.5, 147.5),
            ("Ingresos Corrientes de la Nación", 153.5, 133.5),
            ("Recursos de Capital de la Nación", 11, 9),
            ("Ingresos de los Establecimientos Públicos", 4, 3.5),
            ("Ingresos Corrientes", 3, 2.5),
            ("Recursos de Capital", 1, 1),
            ("TOTAL INGRESOS DEL PGN", 173.5, 151),
        ], **hdr),
        "C2 Ingreso cte": cuadro("Cuadro No. 2.", year, c2 + [("TOTAL INGRESOS CORRIENTES DE LA NACIÓN", total_c2, total_c2_e)], **hdr),
        "C3 Capital": cuadro("Cuadro No. 3.", year, [
            ("Recursos de crédito interno", 10, 8),
            ("Excedentes financieros", 1, 1),
            ("TOTAL RECURSOS DE CAPITAL DE LA NACIÓN", 11, 9),
        ], **hdr),
        "C4 FE": cuadro("Cuadro No. 4.", year, [
            ("Fondo de recursos SOAT y FONSAT", 2, 1.5),
            ("TOTAL FONDOS ESPECIALES DE LA NACIÓN", 2, 1.5),
        ], **hdr),
        "C5 CP": cuadro("Cuadro No. 5.", year, [
            ("Fondo de Prestaciones Sociales del Magisterio", 3, 3.5),
            ("TOTAL CONTRIBUCIONES PARAFISCALES DE LA NACIÓN", 3, 3.5),
        ], **hdr),
    }


FROZEN = pd.DataFrame([
    [2022, "Central", "1.1 Ingresos Corrientes", "01 Ingresos Tributarios", "1 Impuestos Directos", "01 Impuesto Sobre La Renta Y Complementarios", 80, 95],
    [2022, "Central", "1.1 Ingresos Corrientes", "01 Ingresos Tributarios", "2 Impuestos Indirectos", "14 Impuesto Naclonal Al Carbono", 5, 4],
    [2022, "Central", "1.2 Recursos De Capital", "03 Recursos Oe Capital De La Nacion", None, None, 70, 73],
], columns=["year", "admin0", "econ1", "econ2", "econ3", "econ4", "approved", "executed"])


# One entity, executed (code, name, amount) and approved (code, name, amount) lines. Executed
# repeats 1.1.01.01.200 (CUIPO splits a concept by financing source); 1.1.02.03.001 is approved only.
EJEC = [("1", "INGRESOS", 180), ("1.1", "INGRESOS CORRIENTES", 180), ("1.1.01", "INGRESOS TRIBUTARIOS", 180),
        ("1.1.01.01.200", "IMPUESTO PREDIAL UNIFICADO", 100), ("1.1.01.01.200", "IMPUESTO PREDIAL UNIFICADO", 50),
        ("1.1.01.02.200", "IMPUESTO DE INDUSTRIA Y COMERCIO", 30)]
PROG = [("1", "INGRESOS", 170), ("1.1", "INGRESOS CORRIENTES", 170), ("1.1.01", "INGRESOS TRIBUTARIOS", 160),
        ("1.1.01.01.200", "IMPUESTO PREDIAL UNIFICADO", 120), ("1.1.01.02.200", "IMPUESTO DE INDUSTRIA Y COMERCIO", 40),
        ("1.1.02", "INGRESOS NO TRIBUTARIOS", 10), ("1.1.02.03.001", "MULTAS Y SANCIONES", 10)]
TITLE_BLOCK = [["Categoría", "CUIPO"], ["Formulario", "CUIPOB_EJECUCION_DE_INGRESOS"]] + [[None]] * 15


def cuipo_files(style, year):
    """(filename, sheets) pairs for one year of CUIPO ingresos in the given year-style layout."""
    if style == 2021:
        ejec = {"Reduced": [["x"]], "Raw": [
            ["EntidadTerritorialID", "EntidadTerritorialID", "NombreFUT", "CodigoConcepto", "Nombre", "CPC", "Nombre", "TotalRecaudo"],
            *[["110505000 Antioquia", "110505000", "DEPARTAMENTO DE ANTIOQUIA", c, n, "NULL", "NULL", v] for c, n, v in EJEC]]}
        prog = {"Hoja1": [["EntidadTerritorialID", "Nombrefut", "CodigoConcepto", "Nombre", "PresupuestoInicial", "PresupuestoDefinitivo"],
                          *[["110505000", "DEPARTAMENTO DE ANTIOQUIA", c, n, v, v] for c, n, v in PROG]]}
        return [(f"{year}_EJECUCION_DE_INGRESOS.xlsx", ejec), (f"{year}_PROGRAMACION_DE_INGRESOS.xlsx", prog)]
    if style == 2022:
        ejec = {"BOOST": [["x"]], "Ejecución de Ingresos": [
            ["nombreperiodoreporte", "codigodane", "nombreentidad", "concepto_cod", "nombreConcepto", "totalRecaudo"],
            *[["Anual", "05000", "110505000 Antioquia", c, f"{c} {n}", v] for c, n, v in EJEC]]}
        prog = {"Reduced": [["x"]], "Programación de Ingresos": [
            ["nombreperiodoreporte", "CodigoDane", "CodigoFut", "NombreEntidad", "codigoconcepto_cod", "nombreConcepto", "presupuestoInicial", "presupuestoDefinitivo"],
            *[["Anual", "05000", "110505000", "110505000 Antioquia", c, f"{c} {n}", v, v] for c, n, v in PROG]]}
        return [(f"{year}_Ejecucion_de_Ingresos (1).xlsx", ejec), (f"{year} approved.xlsx", prog)]
    ejec = {"Hoja1": TITLE_BLOCK + [
        ["Año Vigencia", "Periodo", "Código DANE", "Código FUT", "Entidad", "Código Concepto", "Concepto", "Total Recaudo"],
        *[[year, "Anual", "05000", "110505000", "Antioquia", c, n, v] for c, n, v in EJEC]]}
    prog = {"Hoja1": TITLE_BLOCK + [
        ["Año Vigencia", "Periodo", "Código DANE", "Código FUT", "Entidad", "Código Concepto", "Concepto", "Presupuesto Inicial", "Presupuesto Definitivo"],
        *[[year, "Anual", "05000", "110505000", "Antioquia", c, n, v, v] for c, n, v in PROG]]}
    return [(f"{year}_Ejecucion de Ingresos_AP_2.xlsx", ejec), (f"{year}_ProgramacionDeIngresos_AP_2.xlsx", prog)]


def fut_file(year):
    header = ["Código FUT", "Nombre Entidad", "Cód. DANE Departamento", "Nombre DANE Departamento", "Cód. DANE Municipio",
              "Nombre DANE Municipio", "Código Concepto", "Concepto", "Presupuesto Inicial", "Presupuesto Definitivo",
              "Recaudo", "Sin Situación Fondos", "Total Ingresos"]
    # (code, name, recaudo, total ingresos, presupuesto definitivo)
    rows = [("TI", "INGRESOS TOTALES", 90, 100, 110), ("TI.A", "INGRESOS CORRIENTES", 90, 100, 110),
            ("TI.A.1", "TRIBUTARIOS", 90, 100, 110), ("TI.A.1.3", "IMPUESTO PREDIAL UNIFICADO", 60, 70, 80),
            ("TI.A.1.5", "IMPUESTO DE INDUSTRIA Y COMERCIO", 30, 30, 30), ("VAL", "INGRESOS TOTALES", 90, 100, 110)]
    return f"{year}_FUTIngresos.xlsx", {"REPORTE_INFORMACION": [header, *[
        ["216605266", "ENVIGADO", "05", "ANTIOQUIA", "05266", "ENVIGADO", c, n, d, d, r, t - r, t] for c, n, r, t, d in rows]]}


def cuadros_path(year):
    return os.path.join(CENTRAL_DIR, f"CUADROS EJECUCION INGRESOS PGN DICIEMBRE {year}.xlsx")


class ColombiaRevenueExtractTest(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TMP, ignore_errors=True)
        os.makedirs(CENTRAL_DIR)
        os.makedirs(AUX_DIR)
        os.makedirs(SUBNAT_DIR)
        FROZEN.to_csv(os.path.join(AUX_DIR, "central_revenue_2019_2022.csv"), index=False)

    def write_subnat(self, files):
        for name, sheets in files:
            write_workbook(os.path.join(SUBNAT_DIR, name), sheets)

    def run_nb(self, cwd=NB_DIR):
        return run_script(NB, env={"RAW_INPUT_DIR": INPUT_ROOT, "OUTPUT_DIR": OUT_DIR}, cwd=cwd)

    def out(self, name):
        return pd.read_csv(os.path.join(OUT_DIR, name))

    def assert_golden(self, name):
        produced = os.path.join(OUT_DIR, name)
        golden = os.path.join(FIXTURES, name)
        if WRITE_GOLDEN:
            shutil.copy(produced, golden)
        pd.testing.assert_frame_equal(pd.read_csv(produced), pd.read_csv(golden))

    def test_central_cuadros_golden(self):
        write_workbook(cuadros_path(2024), cuadros_workbook(2024))
        self.run_nb()
        self.assert_golden("revenue_central_2024.csv")
        df = self.out("revenue_central_2024.csv")
        self.assertEqual(
            set(df[df.econ1 == "Ingresos de los Establecimientos Públicos"].econ4),
            {"Ingresos Corrientes", "Recursos de Capital"})
        self.assertFalse(df.econ4.isin(["Ingresos Corrientes de la Nación", "Recursos de Capital de la Nación", "TOTAL INGRESOS DEL PGN"]).any())
        self.assertAlmostEqual(df.executed.sum(), 151e9)  # C1 TOTAL INGRESOS DEL PGN

    def test_unmapped_central_label_fails(self):
        write_workbook(cuadros_path(2024), cuadros_workbook(2024, extra_c2=[("Impuesto Nuevo de Prueba", 1, 1)]))
        with self.assertRaises(AssertionError) as ctx:
            self.run_nb()
        self.assertIn("Unmapped central revenue line in C2: impuesto nuevo de prueba", str(ctx.exception))

    def test_central_units_and_columns(self):
        write_workbook(cuadros_path(2024), cuadros_workbook(2024))
        write_workbook(cuadros_path(2025), cuadros_workbook(2025, approved_hdr="Vigente", executed_hdr="Recaudo Neto"))
        self.run_nb()
        d24, d25 = self.out("revenue_central_2024.csv"), self.out("revenue_central_2025.csv")
        simple = d24[d24.econ4 == "Impuesto simple"].iloc[0]
        self.assertEqual(simple.executed, 1.5e9)
        self.assertEqual(simple.approved, 1.5e9)
        renta = d24[d24.econ4 == "Impuesto sobre la Renta y Complementarios"].iloc[0]
        self.assertEqual((renta.approved, renta.executed), (100e9, 90e9))
        pd.testing.assert_frame_equal(d24.drop(columns="year"), d25.drop(columns="year"))

    def test_central_concepto_on_second_header_row(self):
        wb = cuadros_workbook(2025)
        wb["C1 Total ingresos"] = cuadro("Cuadro No. 1.", 2025, [
            ("Ingresos del Presupuesto Nacional", 169.5, 147.5),
            ("Ingresos de los Establecimientos Públicos", 4, 3.5),
            ("Ingresos Corrientes", 3, 2.5),
            ("Recursos de Capital", 1, 1),
            ("TOTAL INGRESOS DEL PGN", 173.5, 151),
        ], approved_hdr="Vigente", executed_hdr="Recaudo Neto", concepto_row=1)
        write_workbook(cuadros_path(2025), wb)
        self.run_nb()
        df = self.out("revenue_central_2025.csv")
        self.assertAlmostEqual(df.executed.sum(), 151e9)

    def test_central_categories_and_hierarchy(self):
        write_workbook(cuadros_path(2024), cuadros_workbook(2024))
        self.run_nb()
        df = self.out("revenue_central_2024.csv").set_index("econ4")
        self.assertEqual(df.loc["Impuesto Sobre las Ventas", "econ_rev"], "VAT")
        self.assertEqual(df.loc["Impuesto Sobre las Ventas", "econ3"], "Impuestos Indirectos Internos")
        self.assertEqual(df.loc["Impuesto simple", ["econ1", "econ2", "econ3", "econ_rev"]].tolist(),
                         ["Ingresos Corrientes", "Ingresos Tributarios", "Impuestos Directos", "Income Tax"])
        self.assertEqual(df.loc["Otras Tasas, Multas y Otras Contribuciones", "econ2"], "Ingresos No Tributarios")
        self.assertTrue(pd.isna(df.loc["Otras Tasas, Multas y Otras Contribuciones", "econ3"]))
        self.assertEqual(df.loc["Recursos de crédito interno", "econ1"], "Recursos de Capital")
        # Borrowing is financing, not revenue (decided by the workbook author, 2026-10-21)
        self.assertTrue(df.loc["Recursos de crédito interno", "exclude_from_total"])
        self.assertFalse(df.drop(index="Recursos de crédito interno").exclude_from_total.any())

    def test_frozen_tagged(self):
        self.run_nb()
        self.assert_golden("revenue_central_2022.csv")
        df = self.out("revenue_central_2022.csv")
        carbon = df[df.econ4 == "14 Impuesto Nacional Al Carbono"].iloc[0]
        self.assertEqual((carbon.econ_rev, carbon.econ_rev_sub), ("Excises", "Excises (Fuels)"))
        self.assertIn("03 Recursos De Capital De La Nacion", set(df.econ2))
        self.assertEqual(df[df.econ4 == "01 Impuesto Sobre La Renta Y Complementarios"].econ_rev.iloc[0], "Income Tax")
        self.assertEqual(df.executed.sum(), 172)

    def test_leaf_filter_and_repeat_sum(self):
        self.write_subnat(cuipo_files(2024, 2024))
        self.run_nb()
        df = self.out("revenue_subnational_2024.csv")
        executed = df.dropna(subset=["executed"]).set_index("concept_code").executed.to_dict()
        self.assertEqual(executed, {"1.1.01.01.200": 150, "1.1.01.02.200": 30})

    def test_one_sided_concepts_kept(self):
        self.write_subnat(cuipo_files(2024, 2024))
        self.run_nb()
        row = self.out("revenue_subnational_2024.csv").set_index("concept_code").loc["1.1.02.03.001"]
        self.assertEqual(row.approved, 10)
        self.assertTrue(pd.isna(row.executed))

    def test_cuipo_header_variants(self):
        for style, year in [(2021, 2021), (2022, 2022), (2024, 2024)]:
            self.write_subnat(cuipo_files(style, year))
        self.run_nb()
        frames = [self.out(f"revenue_subnational_{y}.csv").drop(columns="year") for y in (2021, 2022, 2024)]
        pd.testing.assert_frame_equal(frames[0], frames[1])
        pd.testing.assert_frame_equal(frames[0], frames[2])
        self.assertEqual(frames[0].codigo_fut.astype(str).unique().tolist(), ["110505000"])
        self.assertEqual(frames[0].entity_name.unique().tolist(), ["Antioquia"])

    def test_fut_measures(self):
        self.write_subnat([fut_file(2020)])
        self.run_nb()
        df = self.out("revenue_subnational_2020.csv").set_index("concept_code")
        self.assertEqual(df.loc["TI.A.1.3", ["approved", "executed"]].tolist(), [80, 70])  # Total Ingresos, not Recaudo (60)
        self.assertNotIn("VAL", df.index)
        self.assertNotIn("TI", df.index)
        root = self.out("revenue_subnational_root_2020.csv")
        self.assertEqual(root.executed.tolist(), [100])
        self.assertEqual(root.year.tolist(), [2020])

    def test_subnational_golden_2020_2024(self):
        self.write_subnat([fut_file(2020)] + cuipo_files(2024, 2024))
        self.run_nb()
        for name in ["revenue_subnational_2020.csv", "revenue_subnational_2024.csv", "revenue_subnational_root_2024.csv"]:
            self.assert_golden(name)
        df = self.out("revenue_subnational_2024.csv").set_index("concept_code")
        predial = df.loc["1.1.01.01.200"]
        self.assertEqual((predial.econ_rev, predial.exclude_from_total), ("Income Tax", False))
        self.assertTrue(pd.isna(predial.econ_rev_sub))
        self.assertEqual(df.loc["1.1.01.02.200", "econ_rev"], "Subnational Own Revenues")

    def test_non_territorial_entities_dropped(self):
        name, sheets = cuipo_files(2021, 2021)[0]
        sheets["Raw"] += [["923272791 ADRES", "923272791", "ADRES", c, n, "NULL", "NULL", v * 1000] for c, n, v in EJEC]
        self.write_subnat([(name, sheets), cuipo_files(2021, 2021)[1]])
        self.run_nb()
        df = self.out("revenue_subnational_2021.csv")
        self.assertEqual(df.codigo_fut.astype(str).unique().tolist(), ["110505000"])
        self.assertEqual(self.out("revenue_subnational_root_2021.csv").codigo_fut.astype(str).tolist(), ["110505000"])

    def test_root_gap_over_tolerance_fails(self):
        name, sheets = cuipo_files(2024, 2024)[0]
        sheets["Hoja1"] = [r if r[5:6] != ["1"] else r[:7] + [r[7] * 2] for r in sheets["Hoja1"]]  # root executed doubled
        self.write_subnat([(name, sheets), cuipo_files(2024, 2024)[1]])
        with self.assertRaises(AssertionError) as ctx:
            self.run_nb()
        self.assertIn("2024 executed: leaf rows do not add up to the root rows", str(ctx.exception))


def load_notebook_module():
    spec = importlib.util.spec_from_file_location("col_revenue_extract", os.path.join(REPO_ROOT, NB))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # the notebook's I/O runs only under __main__
    return module


class ClassifySubnationalTest(unittest.TestCase):
    PREDIAL = ("Income Tax", None, False)  # kept in Income Tax (workbook author, 2026-10-21)
    FUELS = ("Excises", "Excises (Fuels)", False)
    TOBACCO = ("Excises", "Excises (Tobacco)", False)
    ALCOHOL = ("Excises", "Excises (Alcohol)", False)
    FEES = ("Permits and Fees", None, False)
    OWN = ("Subnational Own Revenues", None, False)
    EXCLUDED = (None, None, True)
    CASES = [
        ("1.1.01.01.200", PREDIAL), ("1.1.01.01.200.01", PREDIAL), ("TI.A.1.3", PREDIAL), ("TI.A.1.3.1", PREDIAL),
        ("1.1.01.02.109", FUELS), ("TI.A.1.26", FUELS),
        ("1.1.01.02.106", TOBACCO), ("TI.A.1.18", TOBACCO),
        ("1.1.01.02.103", ALCOHOL), ("1.1.01.02.104", ALCOHOL), ("1.1.01.02.105", ALCOHOL),
        ("TI.A.1.14", ALCOHOL), ("TI.A.1.15", ALCOHOL), ("TI.A.1.16", ALCOHOL), ("TI.A.1.17", ALCOHOL),
        ("1.1.01.01.100", FEES), ("1.1.01.02.300", FEES), ("1.1.02.02.110", FEES), ("1.1.02.03.001", FEES),
        ("TI.A.1.2", FEES), ("TI.A.1.28", FEES), ("TI.A.2.1.5", FEES), ("TI.A.2.2", FEES),
        ("1.1.02.06.001", EXCLUDED), ("1.1.02.06.003.01", EXCLUDED), ("1.2.06", EXCLUDED), ("1.2.07.01", EXCLUDED),
        ("1.2.08.06.001", EXCLUDED), ("1.2.10", EXCLUDED), ("1.2.12", EXCLUDED),
        ("TI.A.2.6.1", EXCLUDED), ("TI.B.1.1", EXCLUDED), ("TI.B.4", EXCLUDED), ("TI.B.6", EXCLUDED), ("TI.B.10", EXCLUDED),
        ("1.1.01.02.200", OWN), ("TI.A.1.5", OWN), ("1.2.05", OWN), ("TI.B.8", OWN),
        ("1.1.01.01.2001", OWN), ("TI.A.1.30", OWN), ("TI.A.1.2.1", FEES), ("TI.A.1.20", OWN), ("TI.B.11", OWN),
    ]

    def test_classify_subnational(self):
        m = load_notebook_module()
        for code, expected in self.CASES:
            with self.subTest(code=code):
                self.assertEqual(m.classify_subnational(code), expected)

    def test_predial_switch(self):
        m = load_notebook_module()
        with mock.patch.object(m, "PREDIAL_CATEGORY", ("Property Taxes", "Property Taxes (Immovable)")):
            self.assertEqual(m.classify_subnational("1.1.01.01.200"), ("Property Taxes", "Property Taxes (Immovable)", False))
            self.assertEqual(m.classify_subnational("TI.A.1.3"), ("Property Taxes", "Property Taxes (Immovable)", False))



if __name__ == "__main__":
    unittest.main()
