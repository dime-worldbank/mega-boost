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

### How it runs

1. **Extract** (`extract_revenue_to_csv.py`, pandas). The existing
   `extract_raw_microdata_excel_to_csv` task calls it with `%run`. It does all the parsing,
   line-item filtering and categorisation, and writes CSVs to
   `Workspace/raw_microdata_csv/Colombia/`:
   - `revenue_central_{year}.csv`
   - `revenue_subnational_{year}.csv`
   - `revenue_subnational_root_{year}.csv`: each entity's root-row totals, used for checks
2. **Transform** (`transform_load_revenue_dlt.py`, DLT, in the Colombia pipeline). It loads the
   CSVs, adds geography for subnational rows, and builds:
   - `col_rev_central_bronze`, `col_rev_subnat_bronze`
   - `col_rev_subnat_silver`: adds department/municipality via
     `indicator_intermediate.col_subnational_adm2_adm1_lookup`, using the same FUT-code →
     DANE rule as expenditure
   - `col_boost_rev_gold`
   - `col_rev_quality`
3. **Tests** (`tests/test_colombia_revenue_extract.py`) run the extract locally against
   synthetic workbooks in each year's layout.

Gold columns: `country_name, year, admin0, admin1, admin2, geo1, econ1–econ4, concept_code,
econ_rev, econ_rev_sub, exclude_from_total, approved, executed`.
- `admin1`/`admin2` mean who collected the revenue, following the cross-country BOOST schema:
  central rows are `Central Scope`; subnational rows are department, then municipality, with
  `admin2` null for a department government's own revenue.
- `geo1` is the department.
- Total revenue = the sum of rows where `exclude_from_total` is false.

### Quality checks

- **Extract (fails the task):**
  - each Cuadros sheet's lines equal its TOTAL row;
  - subnational line items equal root rows per year, within 0.01%;
  - every C2 label is in the map;
  - header layouts are recognised.
- **DLT `col_rev_quality` (`expect_or_fail`):**
  - per year, subnational executed equals the root rows, within 0.01%;
  - every subnational row has a department.

### Sources by year

All raw files live in the BOOSTProcessed SharePoint under
`input/Data from authorities/Colombia/`, which syncs automatically to the Databricks volume.

| Level | Years | File(s) | Approved | Executed |
|---|---|---|---|---|
| Central | 2019–2022 | `auxiliary/central_revenue_2019_2022.csv` | `approved` | `executed` |
| Central | 2023 | — (not published by MinHacienda; not found elsewhere) | | |
| Central | 2024+ | `central/revenues/CUADROS EJECUCION INGRESOS PGN DICIEMBRE {year}.xlsx`, sheets C2–C5 plus the Establecimientos Públicos block of C1 | col 3 "Definitivo"/"Vigente" | col 4 "Recaudo"/"Recaudo Neto" |
| Subnational | 2019–2020 | `subnational/ingresos/{year}_FUTIngresos.xlsx` | `Presupuesto Definitivo` | `Total Ingresos` |
| Subnational | 2021+ | `subnational/ingresos/`: a file with "ejecucion" in its name (executed) and one with "programacion" or "approved" (approved) | `Presupuesto Definitivo` | `Total Recaudo` |

### Adding a year

1. Download the files listed under *Sources by year* into BOOSTProcessed (README sections
   above give the download steps). Name subnational files with the year first and "ejecucion" /
   "programacion" in the name.
2. Run the Colombia job. If the extract fails on `Unmapped central revenue line in C2: …`, add
   that label to `central_revenue_lines.csv` (normalized: lowercase, no accents or punctuation)
   with its category, or leave the category blank. If it fails on a header layout, extend the
   header detection and add a test fixture in that year's layout.
3. If the workbook needs the new year, add its year column to the REV rows.

### Decisions: central

- **Central 2019–2022 come from a frozen copy.** The only source for those years is an OCR'd
  PDF. The workbook's `Revenue` sheet holds its hand-checked central lines; those 104 rows are
  copied unmodified into `auxiliary/central_revenue_2019_2022.csv`.
  - It's data, so it isn't committed to git.
  - It isn't read from the published workbook (`input/Countries/Colombia BOOST.xlsx`) because
    that workbook's `Revenue` sheet is regenerated from this pipeline, which would make the
    pipeline read its own output.
  - OCR typos in its labels are fixed in code at read time (`OCR_WORD_FIXES`,
    `OCR_TRUNCATED_ENDINGS`); the numbers are untouched. Fixes: `Oe`→`De`,
    `Naclonal`→`Nacional`, `Pijblicos`→`Públicos`, `Rjqljeza`→`Riqueza`, `O8`→`08`,
    `PatriMonio`→`Patrimonio`, `Aduanas Y`→`Aduanas Y Recargos`.
- **Central lines are categorised by label** via `central_revenue_lines.csv`, because the
  MinHacienda tables have no codes.
  - Every C2 (current revenue) line must be listed. Any unlisted line fails the extract with
    the label named, so a new tax (e.g. 2025's *Impuesto Especial para el Catatumbo*) is never
    silently dropped. Add it to the map and rerun.
  - Sheets C1, C3, C4 and C5 use sheet-wide defaults.
  - Each sheet's lines must add up to its TOTAL row.
- **Central includes Establecimientos Públicos** (from sheet C1), matching the frozen
  2019–2022 line *Ingresos de los Establecimientos Públicos*. Central totals therefore equal
  MinHacienda's *TOTAL INGRESOS DEL PGN* (400.91 tn COP in 2024, 482.50 tn in 2025).
- **Central borrowing is excluded from total revenue** (workbook author, 2026-10-21). C3's
  *Recursos de crédito interno/externo* get `exclude_from_total`. This applies from 2024 only,
  because 2019–2022 central capital resources are one undivided line that includes borrowing,
  so expect a step down in central total revenue between 2022 and 2024.

### Decisions: subnational

- **Departments and municipalities only** (FUT entity codes starting `11` and `21`). The 2021
  CUIPO files list every public entity that reports to CUIPO, including Ecopetrol, ADRES,
  Colpensiones, pension funds and hospitals, which would more than quadruple 2021's approved
  total. Other years carry 5–7 such entities (about 0.2–0.45 tn COP a year). All of them are
  dropped, consistent with expenditure.
- **Line items only.** Source files list every level of the concept tree. The extract keeps
  rows whose code has no children for that entity (summing repeats split by financing
  source/CPC), and checks that they add up to the entity's root row (`TI` / `1`). The check is
  per year, within 0.01%; entities that differ are printed.
- **Executed for FUT years is `Total Ingresos`** (`Recaudo` plus *sin situación de fondos*).
  It matches the workbook's earlier local totals; `Recaudo` alone is about 20% lower.
- **Categories come from concept codes** (`SUBNATIONAL_RULES`, longest prefix wins), covering
  CUIPO (2021+) and FUT (2019–2020). Lines with no specific category are
  `Subnational Own Revenues`.
- **Excluded from total revenue** (`exclude_from_total`): intergovernmental current transfers
  (CUIPO `1.1.02.06`, all of it, including SGP; FUT `TI.A.2.6`), capital transfers and
  cofinancing (`1.2.08`, `TI.B.1`), borrowing (`1.2.06`, `1.2.07`, `TI.B.4`), prior-year
  balances (`1.2.10`, `TI.B.6`) and FONPET withdrawals (`1.2.12`, `TI.B.10`). Together that's
  about 60% of subnational revenue. The rows are kept and flagged rather than dropped.
- **Impuesto Predial stays in Income Tax** (workbook author, 2026-10-21;
  `PREDIAL_CATEGORY = ('Income Tax', None)`), now for every year. The old workbook formula
  picked it up only in 2021–2022.

### Known breaks and source quirks

- **VAT vs Customs:** in the 2019–2022 tables, *Impuesto sobre aduanas y recargos* includes VAT
  on imports (IVA externo), roughly 24–44 tn COP a year. From 2024 MinHacienda reports IVA
  externo separately, and it's counted as VAT. So Customs drops to about 5–6 tn and VAT rises by
  the same amount. The frozen years can't be split.
- **C1 header layout varies:** "Concepto" sits on either header row depending on the year, so
  the parser anchors on the column-number row `(3)=(1+2)` instead.
- **Small root mismatches:** one 2019 entity's FUT `TI.A.2` approved exceeds its children by
  2.55 bn COP, and one 2022 entity has a rounding-sized executed gap. Both are within tolerance
  and are printed.
