## Data Source

### central: 

#### Budget & Expenditure:

https://www.minhacienda.gov.co/es/web/pte/presupuesto-general-nacion/seguimiento-ejecucion-presupuestal-gastos/

- Select year
- Select Diciembre (December)
- Find 02. Cuadros informe de ejecución Diciembre (02. Execution report tables December) > Download

In OneDrive:

- Move it into Colombia public > Data from authorities > central > egresos > other, rename as year.xlsx e.g. 2024.xlsx
- Open it in excel and save datasheet "Cuadro No. 7" to its own file named e.g. "2024 Ejecución Presupuesto General de la Nación detalle por Sector, entidad y rubro.xlsx" in the parent folder "egresos"

#### Revenue:

https://www.minhacienda.gov.co/es/web/pte/ingreso-seguimiento-presupuestal

- Select year
- Select Diciembre (December)
- PGN INCOME EXECUTION TABLES DECEMBER 2024 > Download

Save to OneDrive > Colombia public > Data from authorities > central > revenues

### subnational: 

https://www.dnp.gov.co/LaEntidad_/subdireccion-general-descentralizacion-desarrollo-territorial/direccion-descentralizacion-fortalecimiento-fiscal/Paginas/informacion-fiscal-y-financiera.aspx

#### Budget & Expenditure:

Example files for 2024:
- 2024_Ejecución de Gastos.xlsx
- 2024_Programación de Gastos.xlsx

Save to OneDrive > Colombia public > Data from authorities > subnational > gastos

#### Revenue:

Examples:
- 2024_Ejecución de Ingresos_AP_2.xlsx
- 2024_ProgramaciónDeIngresos_AP_2.xlsx

Save to OneDrive > Colombia public > Data from authorities > subnational > ingresos

## Revenue pipeline

Builds `prd_mega.boost_intermediate.col_boost_rev_gold`: Colombia central and subnational
revenue from 2019 onward, one row per revenue line, tagged with BOOST revenue categories
(`econ_rev` / `econ_rev_sub`, the same set as Albania's `alb_boost_rev_gold`). The Colombia
BOOST workbook's REV indicators (Approved/Executed rows 299–320) are computed from this table.

### Sources by year

All raw files live in the BOOSTProcessed SharePoint under
`input/Data from authorities/Colombia/`, which syncs automatically to the Databricks volume.

| Level | Years | File(s) | Approved | Executed |
|---|---|---|---|---|
| Central | 2019–2022 | `auxiliary/central_revenue_2019_2022.csv` | `approved` | `executed` |
| Central | 2023 | — (not published by MinHacienda; not found elsewhere) | | |
| Central | 2024+ | `central/revenues/CUADROS EJECUCION INGRESOS PGN DICIEMBRE {year}.xlsx` | col 3 "Definitivo"/"Vigente" | col 4 "Recaudo"/"Recaudo Neto" |
| Subnational | 2019–2020 | `subnational/ingresos/{year}_FUTIngresos.xlsx` | `Presupuesto Definitivo` | `Total Ingresos` |
| Subnational | 2021+ | `subnational/ingresos/` CUIPO ejecución + programación files | `Presupuesto Definitivo` | `Total Recaudo` |

### Decisions

- **Central 2019–2022 come from a frozen copy.** The only source for those years is an OCR'd
  PDF. The workbook's `Revenue` sheet holds its hand-checked central lines; those 104 rows are
  copied unmodified into `auxiliary/central_revenue_2019_2022.csv`.
  - It's data, so it isn't committed to git.
  - It isn't read from the published workbook (`input/Countries/Colombia BOOST.xlsx`) because
    that workbook's `Revenue` sheet is regenerated from this pipeline, which would make the
    pipeline read its own output.
  - OCR typos in its labels are fixed in code at read time (`OCR_LABEL_FIXES` in
    `extract_revenue_to_csv.py`); the numbers are untouched. Fixes: `Oe`→`De`,
    `Naclonal`→`Nacional`, `Pijblicos`→`Públicos`, `Rjqljeza`→`Riqueza`, `O8`→`08`,
    `PatriMonio`→`Patrimonio`, `Aduanas Y`→`Aduanas Y Recargos`.
- **Central lines are categorised by label** via `central_revenue_lines.csv`, because the
  MinHacienda tables have no codes. Any Cuadros line not in the map fails the extract with
  the label named, so a new tax (e.g. 2025's *Impuesto Especial para el Catatumbo*) is never
  silently dropped. Add it to the map and rerun.
- **Central includes Establecimientos Públicos** (from sheet C1), matching the frozen
  2019–2022 line *Ingresos de los Establecimientos Públicos*. Central totals therefore equal
  MinHacienda's *TOTAL INGRESOS DEL PGN*.

### Decisions from the workbook author (2026-10-21)

Both are single switches at the top of `extract_revenue_to_csv.py`.

1. **Central borrowing is excluded from total revenue** (`EXCLUDE_CENTRAL_BORROWING = True`):
   C3's *Recursos de crédito interno/externo* get `exclude_from_total`. This only applies to
   2024 onward, because 2019–2022 central capital resources are one undivided line that
   includes borrowing. Expect a step down in central total revenue between 2022 and 2024.
2. **Impuesto Predial stays in Income Tax** (`PREDIAL_CATEGORY = ('Income Tax', None)`), now
   for every year. The old workbook formula picked it up only in 2021–2022.
- **Known break, VAT vs Customs:** in the 2019–2022 tables, *Impuesto sobre aduanas y
  recargos* includes VAT on imports (IVA externo), roughly 24–44 tn COP a year. From 2024
  MinHacienda reports IVA externo separately, and it's counted as VAT. So Customs drops to
  about 5–6 tn and VAT rises by the same amount. The frozen years can't be split.
- **C1 header layout varies:** "Concepto" sits on either header row depending on the year,
  so the parser anchors on the column-number row `(3)=(1+2)` instead.
- **Subnational = departments and municipalities only** (FUT entity codes starting `11` and
  `21`). The 2021 CUIPO files list every public entity that reports to CUIPO, including
  Ecopetrol, ADRES, Colpensiones, pension funds and hospitals, which would more than quadruple
  2021's approved total. Other years carry 5–7 such entities (about 0.2–0.45 tn COP a year).
  All of them are dropped, consistent with expenditure.
- **Line items only.** Source files list every level of the concept tree. The extract keeps
  rows whose code has no children for that entity (summing repeats split by financing
  source/CPC), and checks that they add up to the entity's root row (`TI` / `1`). The check is
  per year, within 0.01%; entities that differ are printed. Known source quirk: one 2019
  entity's FUT `TI.A.2` approved exceeds its children by 2.55 bn COP.
- **Executed for FUT years is `Total Ingresos`** (`Recaudo` plus *sin situación de fondos*).
  It matches the workbook's earlier local totals; `Recaudo` alone is about 20% lower.
- **Subnational categories come from concept codes** (`SUBNATIONAL_RULES`, longest prefix
  wins), covering CUIPO (2021+) and FUT (2019–2020) codes. Lines with no specific category are
  `Subnational Own Revenues`.
- **Excluded from total revenue** (`exclude_from_total`): intergovernmental current transfers
  (CUIPO `1.1.02.06`, all of it, including SGP; FUT `TI.A.2.6`), capital transfers and
  cofinancing (`1.2.08`, `TI.B.1`), borrowing (`1.2.06`, `1.2.07`, `TI.B.4`), prior-year
  balances (`1.2.10`, `TI.B.6`) and FONPET withdrawals (`1.2.12`, `TI.B.10`). Together that's
  about 60% of subnational revenue. The rows are kept and flagged rather than dropped.
