# Colombia BOOST revenue pipeline — design

Date: 2026-10-20 · Branch: `col-revenue` (stacked on PR #71 `col-central-2025`)

## Why

The Colombia BOOST workbook computes 22 revenue indicators (`REV_ECON_*`, rows 299–320 of
the Approved and Executed sheets) from a hand-built `Revenue` sheet. They stop at 2022, and
the formulas match exact label strings, including OCR typos from a scanned 2019–22 PDF
("Naclonal", "Aduanas Y"). Clean label text for new years would silently match nothing.

We want:
1. A pipeline table of Colombia revenue (central + subnational) built from source, tagged with
   BOOST revenue categories, like Albania's `alb_boost_rev_gold`.
2. The workbook's REV rows filled for the new years, computed by category instead of label text.

## Goals

- `prd_mega.boost_intermediate.col_boost_rev_gold`: one row per revenue line, 2019 onward.
- Subnational revenue keeps entity geography (department / municipality), like expenditure.
- Every line carries `econ_rev` / `econ_rev_sub` (BOOST categories) and an
  exclusion flag for total revenue.
- Workbook REV rows (2019 onward) rewritten to sum by category; 2019–2022 shifts reported to
  the user **before** the workbook is changed.

## Non-goals

- Central 2023 (no source available — decided to live without it).
- Years before 2019 (expenditure starts in 2019).
- Adding revenue to `cross_country_aggregate_dlt.py` (Albania revenue isn't there either).
- Central public establishments by entity (sheet C6) and the C7 detail sheet — later if needed.

## Anti-goals

- Double counting: aggregate rows ("1 INGRESOS", "1.1 INGRESOS CORRIENTES", …) summed together
  with their children; or intergovernmental transfers counted at both levels.
- Silent zeroes: a category that maps nothing in a year where source data exists.
- Changing published 2019–2022 workbook values without the user seeing the shift list first.

## Sources (all in the volume under `Data from authorities/Colombia/`, synced from the BOOSTProcessed SharePoint)

| Level | Years | Executed | Approved | Notes |
|---|---|---|---|---|
| Central | 2019–2022 | frozen reference CSV (below) | same | from the workbook's `Revenue` sheet, Central rows |
| Central | 2024–2025 | `central/revenues/CUADROS EJECUCION INGRESOS PGN DICIEMBRE {year}.xlsx` sheets C2–C5, "Recaudo" | "Aforo Definitivo"/"Vigente" | billions of pesos (×1e9). C1 header says "Definitivo" (2024) vs "Vigente" (2025) |
| Subnational | 2019–2020 | `subnational/ingresos/{year}_FUTIngresos.xlsx` `Total Ingresos` (= Recaudo + Sin Situación de Fondos; matches the workbook's local totals within ~1.5%, `Recaudo` alone is ~20% lower) | `Presupuesto Definitivo` (same file) | FUT codes `TI.A.1.1.1…` (root row `TI`; `VAL` rows are validation — drop); DANE dept + municipio codes |
| Subnational | 2021 | `2021_EJECUCION_DE_INGRESOS.xlsx` sheet `Raw`, `TotalRecaudo` | `2021_PROGRAMACION_DE_INGRESOS.xlsx` `PresupuestoDefinitivo` | CUIPO codes; entity = `EntidadTerritorialID` (FUT code) |
| Subnational | 2022 | `2022_Ejecucion_de_Ingresos (1).xlsx` sheet `Ejecución de Ingresos`, `totalRecaudo` | `2022 approved.xlsx` sheet `Programación de Ingresos`, `presupuestoDefinitivo` | other sheets in these workbooks are analyst pivots — ignore |
| Subnational | 2023–2024 | `{year}_Ejecucion*Ingresos*.xlsx`, header on row 18, `Total Recaudo` | `{year}_Programacion*Ingresos*.xlsx`, `Presupuesto Definitivo` | CUIPO export with title block above header |

Subnational 2025 is not published by DNP yet; the pipeline picks it up when files land.

### Frozen central 2019–2022

`auxiliary/central_revenue_2019_2022.csv` (copied into the CSV folder by the extract step,
like the other auxiliary files): the workbook `Revenue` sheet's Central rows for 2019–2022
(`year, econ1, econ2, econ3, econ4, approved, executed`), with OCR typos fixed **in labels
only**; numbers untouched. A label-fix mapping (old → new) is kept in the PR description so
reviewers can see each change.

## Shape of `col_boost_rev_gold`

`country_name, year, admin0 ('Central'|'Regional'), admin1, admin2, geo1, econ1…econ4,
concept_code, econ_rev, econ_rev_sub, exclude_from_total (bool), approved, executed`

Admin columns follow the cross-country BOOST schema meaning (`cross_country_aggregate_dlt.py`):
`admin1`/`admin2` = **who collected** the revenue, `geo1` = department (decided with the user;
Albania's revenue table uses `admin1` = 'Central'/'Local', which we don't copy).

- Central: `admin1 = 'Central Scope'`, `geo1 = 'Central Scope'` (as expenditure), `admin2` null
  (central revenue sources aren't by entity).
- Subnational: `admin1`/`admin2` = department / municipality names via the same
  `indicator_intermediate.col_subnational_adm2_adm1_lookup` (DANE `nso_code`) used by expenditure.
  Departmental governments have `admin2` null.
- Approved and executed come from separate files for CUIPO years; join on
  (entity, concept code). Lines present in only one side keep null on the other.

## Line items vs aggregates

Source files list every level of the concept tree. Keep only leaves: a row whose concept code
is not a prefix (followed by `.`) of another code **for the same entity and year**. CUIPO also
repeats a concept across financing source / CPC / sector detail — those repeats are summed per
(entity, concept code) after leaf filtering.

Check: per entity, sum of leaves = that entity's root row ("1 INGRESOS" / FUT total row) for
executed and approved, within rounding.

## Categories (`econ_rev`, `econ_rev_sub`)

Same category set as Albania and the workbook's REV rows:
Income Tax (Corporate / Personal), VAT, Excises (Fuels / Tobacco / Alcohol), Customs
(Imports / Exports), Royalties, Permits and Fees, Social Contributions, Property Taxes
(Immovable / Transactions), Subnational Own Revenues.

Starting rules = what the workbook formulas do today, re-expressed on clean labels / codes:

| Category | Central (labels) | Subnational (CUIPO / FUT) |
|---|---|---|
| Income Tax | Impuesto sobre la renta y complementarios; Impuesto Simple | — |
| VAT | Impuesto sobre las ventas | — |
| Customs | Impuesto sobre aduanas y recargos | — |
| Excises — Fuels | Impuesto nacional a la gasolina y ACPM; Impuesto nacional al carbono | Sobretasa a la gasolina |
| Excises — Tobacco | — | Impuesto al consumo de cigarrillos y tabaco |
| Excises — Alcohol | — | Impuesto al consumo de licores/vinos/aperitivos; cerveza; IVA licores |
| Royalties | Impuesto sobre explotación de oro, plata y platino | — |
| Permits and Fees | Tasas y derechos administrativos; multas, sanciones e intereses; timbre nacional; timbre salidas al exterior | Estampillas; vehículos automotores; tasas y derechos; multas y sanciones |
| Property Taxes | Impuesto al patrimonio | **Impuesto Predial Unificado → Property Taxes (Immovable)** |
| Subnational Own Revenues | — | every subnational line not excluded from total |

**Predial (my call, pending the user's email to the workbook author):** mapped to Property
Taxes (Immovable). Today's workbook counts it in Income Tax for 2021–2022 only (label/column
mismatch excludes it for 2019–2020). If the author says income tax was intended, it's a
one-line change.

`exclude_from_total` = true for: Sistema General de Participaciones (SGP) transfers,
capital transfers, borrowing (recursos del crédito interno/externo), recursos del balance,
FONPET withdrawals — the workbook's current total-revenue exclusions.

**Central borrowing — decision pending (see Open questions).** In the workbook today these
exclusions only hit local codes; central "Recursos de Capital" is one undivided line for
2019–2022 (73.5 tn COP executed in 2022), so central borrowing is *inside* total revenue.
Sheet C3 splits it for 2024–2025: crédito interno + externo = 74.8 tn of 102.8 tn (2024) and
128.6 tn of 155.1 tn (2025).

Rules live in the transform as `when(...)` chains on concept code where codes exist
(CUIPO, FUT) and on normalized labels for central. Any subnational line with a tax-looking code
(`1.1.01.*` CUIPO / `TI.A.*` FUT) that maps to no category is listed by the quality check (see below).

## Where it runs

- Extract: extend `Colombia/extract_raw_microdata_excel_to_csv.py` with a revenue section writing
  `central_revenue_{year}.csv`, `subnational_ingresos_ejecucion_{year}.csv`,
  `subnational_ingresos_programacion_{year}.csv` (skip-if-exists like subnational gastos).
- Transform: new revenue section in the existing Colombia DLT pipeline (`910c7eb9…`), as a second
  notebook `Colombia/transform_load_revenue_dlt.py` added to the pipeline's libraries — same
  engine as Colombia expenditure (my call: one Colombia pipeline, not a second engine).
  Adding the notebook to the pipeline's library list is a one-time pipeline setting change.
- No change to the job DAG.

## Quality checks (expect_or_fail, like `col_subnat_quality`)

1. Central 2024–2025: sum by C1 heading (corrientes, capital, fondos especiales, parafiscales)
   = C1 totals.
2. Subnational: leaves sum to each entity's root row (both measures).
3. Frozen central 2019–2022: totals equal the workbook `Revenue` sheet's Central totals.
4. Unmapped tax-code lines report (warning table, not failing).

## Workbook update

- `Revenue` sheet: rows for 2019 onward replaced by `col_boost_rev_gold` aggregated nationally per
  (year, admin0, econ1–econ4, econ_rev, econ_rev_sub, exclude_from_total), with new columns
  `econ_rev`, `econ_rev_sub`, `exclude_from_total`. Pre-2019 rows (central 2006–2018, local
  2000–2018) stay as they are.
- REV rows 299–320, year columns 2019 onward: `SUMIFS` by `econ_rev` / `econ_rev_sub` and
  `exclude_from_total`, replacing label lists. **2006–2018 columns keep their existing formulas**
  (my call: those years aren't in the pipeline and their rows aren't category-tagged).
- **Before writing**: shift report per REV row × year (old value, new value, difference, and
  the lines that moved), sent to the user for sign-off.

## Left to the builder

- Exact label normalization for central (accents/case/whitespace).
- FUT code → category rules (FUT uses its own code tree).
- How to read the CUIPO title block (header-row detection, as the gastos extract does).

## Open questions (emailed to the workbook author 2026-10-20)

Both are implemented as single switches so the build doesn't wait on the answers.

1. Central borrowing in total revenue for 2024–2025. Default until answered: **keep it in**
   (matches 2019–2022); switch = mark central `Recursos de crédito interno/externo` as
   `exclude_from_total`.
2. Predial: default **Property Taxes (Immovable)**; switch = map to Income Tax.
