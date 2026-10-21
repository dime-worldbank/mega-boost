"""Integration tests for Colombia/extract_revenue_to_csv: run the notebook end to end against
fixture inputs shaped like the real MinHacienda / FUT / CUIPO files and compare each produced
CSV to a committed golden file. All fixture values are synthetic.

Set WRITE_GOLDEN=1 to (re)generate the golden CSVs after an intentional change.
"""
import os
import shutil
import unittest

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


def cuadros_path(year):
    return os.path.join(CENTRAL_DIR, f"CUADROS EJECUCION INGRESOS PGN DICIEMBRE {year}.xlsx")


class ColombiaRevenueExtractTest(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TMP, ignore_errors=True)
        os.makedirs(CENTRAL_DIR)
        os.makedirs(AUX_DIR)
        FROZEN.to_csv(os.path.join(AUX_DIR, "central_revenue_2019_2022.csv"), index=False)

    def run_nb(self):
        return run_script(NB, env={"RAW_INPUT_DIR": INPUT_ROOT, "OUTPUT_DIR": OUT_DIR}, cwd=NB_DIR)

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
        self.assertFalse(df.exclude_from_total.any())

    def test_frozen_tagged(self):
        self.run_nb()
        self.assert_golden("revenue_central_2022.csv")
        df = self.out("revenue_central_2022.csv")
        carbon = df[df.econ4 == "14 Impuesto Nacional Al Carbono"].iloc[0]
        self.assertEqual((carbon.econ_rev, carbon.econ_rev_sub), ("Excises", "Excises (Fuels)"))
        self.assertIn("03 Recursos De Capital De La Nacion", set(df.econ2))
        self.assertEqual(df[df.econ4 == "01 Impuesto Sobre La Renta Y Complementarios"].econ_rev.iloc[0], "Income Tax")
        self.assertEqual(df.executed.sum(), 172)


if __name__ == "__main__":
    unittest.main()
