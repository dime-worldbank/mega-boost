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

### Open questions (sent to the workbook author, 2026-10-20)

Both are single switches at the top of `extract_revenue_to_csv.py`.

1. **Central borrowing in total revenue** (`EXCLUDE_CENTRAL_BORROWING`, default `False`).
   2019–2022 can't separate borrowing from other capital resources, so it's kept in for
   consistency until answered.
2. **Impuesto Predial** (`PREDIAL_CATEGORY`, default Property Taxes (Immovable)). The old
   workbook formula counted it under Income Tax in 2021–2022 only.
