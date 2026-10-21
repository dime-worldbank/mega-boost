# Colombia Revenue Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `prd_mega.boost_intermediate.col_boost_rev_gold` (Colombia central + subnational revenue, 2019 onward, BOOST revenue categories) and rewrite the workbook's REV rows to use it.

**Architecture:** All parsing, leaf-filtering and categorisation happen in one pandas notebook, `Colombia/extract_revenue_to_csv.py`. It runs locally the way Albania's extract does and writes tagged CSVs. A thin DLT notebook, `Colombia/transform_load_revenue_dlt.py`, added to the existing Colombia pipeline, loads those CSVs, attaches geography and checks totals. The workbook update is a one-off script outside the repo, run after the user signs off on a shift report.

**Tech Stack:** pandas 1.5.3 / openpyxl (pinned in `tests/requirements.txt`), unittest, Databricks DLT (PySpark), xlwings (workbook step only).

**Spec:** `docs/superpowers/specs/2026-10-20-col-revenue-pipeline-design.md`

## Global Constraints

- Years: 2019 onward; central 2023 absent; central 2019–2022 come only from the frozen CSV.
- Central Cuadros values are *miles de millones* → multiply by `1e9`. Approved = column 3 ("Definitivo" in 2024, "Vigente" in 2025); executed = column 4 ("Recaudo" / "Recaudo Neto").
- Subnational executed: FUT `Total Ingresos`; CUIPO `TotalRecaudo` / `totalRecaudo` / `Total Recaudo`. Approved: `Presupuesto Definitivo` (any casing/spacing).
- Admin columns use the cross-country schema meaning: `admin1`/`admin2` = who collected; `geo1` = department. Central: `admin1 = geo1 = 'Central Scope'`, `admin2` null.
- Switches (module-level constants in the extract notebook): `EXCLUDE_CENTRAL_BORROWING = False`; `PREDIAL_CATEGORY = ('Property Taxes', 'Property Taxes (Immovable)')`.
- No `unidecode` (not pinned in tests): use `unicodedata` for accent stripping.
- Follow repo patterns: `IS_DATABRICKS` / `RAW_INPUT_DIR` / `OUTPUT_DIR` env (see `Albania/ALB_extract_raw_microdata_excel_to_csv.py:13-20`); tests via `tests/helpers.py` `run_script` + `write_workbook`; golden CSVs under `tests/fixtures/colombia/` with `WRITE_GOLDEN=1`.
- Commits: short subject, minimal body, `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, no `Claude-Session` line. Never push without asking.

## Review Focus

1. **New central line labels** (e.g. 2025's "Impuesto Especial para el Catatumbo") must fail the extract, naming the label. They must never drop silently → Task 2 test `test_unmapped_central_label_fails`.
2. **Parent rows plus repeated detail rows** (same concept split by financing source or CPC) must be counted exactly once → Task 3 test `test_leaf_filter_and_repeat_sum`.
3. **A concept on only one side** (approved-only or executed-only) must keep its row, with null on the missing side, and not vanish in the join → Task 3 test `test_one_sided_concepts_kept`.
4. **Header variants across years** (2021 `Raw` sheet, 2022 analyst workbook, 2023/24 title block above row 18) must all resolve to the same columns → Task 3 test `test_cuipo_header_variants`.
5. **Department governments vs municipalities**: a department's own revenue must get `admin2` null, not be attributed to a capital city → Task 5 check `admin2 IS NULL` for FUT codes ending in `000`.

---

### Task 1: Frozen central 2019–2022 (auxiliary data) and the central line map

**Files:**
- Data (not in git): `BOOSTProcessed/input/Data from authorities/Colombia/auxiliary/central_revenue_2019_2022.csv`, local path `~/Library/CloudStorage/OneDrive-WBG/Documents - BOOSTProcessed/…`. It's an unmodified copy of workbook sheet `Revenue` rows with `admin0 == 'Central'` and `2019 <= year <= 2022` (104 rows, columns `year,admin0,econ1,econ2,econ3,econ4,approved,executed`). It syncs to the volume `auxiliary/` folder, which the existing extract copies into the CSV dir.
- Create: `Colombia/central_revenue_lines.csv` (code-like config: label → hierarchy/category) with columns `sheet,label,econ1,econ2,econ3,econ_rev,econ_rev_sub,is_subtotal`
- Modify: `Colombia/README.md`: add a "Revenue pipeline" section (design, sources per year, decisions and why, open questions). It grows with each task.

**Interfaces:**
- Produces: the aux CSV above; the line map; `OCR_LABEL_FIXES: dict[str, str]` in the extract notebook (Task 2), applied to the frozen labels at read time. `label` in the line map is matched after `normalize_label` (Task 2). `sheet` is one of `C1,C2,C3,C4,C5,FROZEN`, or `*`, which applies to any sheet. `label='*'` is the sheet-wide default.

Label fixes (old → new, word-level), kept in code: `Oe → De`, `Naclonal → Nacional`, `Pijblicos → Públicos`, `Rjqljeza → Riqueza`, `O8 → 08`, `Aduanas Y → Aduanas Y Recargos` (whole-label match only), `PatriMonio → Patrimonio`. The executor adds any others found by scanning the 104 rows, and lists them in the README.

Line map rows (`econ_rev`, `econ_rev_sub`; blank = no category):

| sheet | label (normalized) | econ_rev | econ_rev_sub |
|---|---|---|---|
| * | impuesto sobre la renta y complementarios | Income Tax | |
| * | impuesto simple | Income Tax | |
| * | impuesto sobre las ventas / impuesto sobre las ventas (iva externo) | VAT | |
| * | impuesto sobre aduanas y recargos | Customs | |
| * | impuesto nacional a la gasolina y al acpm / impuesto nacional al carbono | Excises | Excises (Fuels) |
| * | impuesto a la explotacion de oro, plata y platino | Royalties | |
| * | impuesto de timbre nacional / impuesto de timbre nacional sobre salidas al exterior / tasas y derechos administrativos / multas, sanciones e intereses de mora / otras tasas, multas y otras contribuciones | Permits and Fees | |
| * | impuesto al patrimonio | Property Taxes | |
| C2 | ingresos tributarios / impuestos directos / impuestos indirectos internos / impuestos indirectos externos (iva externo y aduanas y recargos) / ingresos no tributarios / total ingresos corrientes de la nacion | `is_subtotal=1` | |
| C1 | the four rows under `ingresos de los establecimientos publicos` (ingresos corrientes, recursos de capital, fondos especiales establecimientos publicos, contribuciones parafiscales de los establecimientos publicos) | (none) | |
| C1 | every other C1 row | `is_subtotal=1` (C1 is only read for the Establecimientos Públicos block; the Nación rows come from C2–C5) | |
| C3/C4/C5 | `*` | (none) | |
| C3/C4/C5 | label starting `total ` | `is_subtotal=1` | |

Hierarchy (`econ1`/`econ2`/`econ3`) for C2 rows comes from the subtotal they sit under: econ1 `Ingresos Corrientes`, econ2 `Ingresos Tributarios`/`Ingresos No Tributarios`, econ3 the `Impuestos …` subtotal. C3: econ1 `Recursos de Capital`. C4: econ1 `Fondos Especiales`. C5: econ1 `Contribuciones Parafiscales`. C1 block: econ1 `Ingresos de los Establecimientos Públicos` (matches the frozen 2019–2022 line `08 Ingresos De Los Establecimientos Públicos`). In every case econ4 = the line label.

- [ ] **Step 1: Copy the aux CSV** to BOOSTProcessed and wait for it to show up in the volume (`databricks fs ls …/Colombia/auxiliary`). Verify: 104 rows; executed sums 2019 237.43e12, 2020 288.54e12, 2021 320.71e12, 2022 329.86e12 (±0.01e12).
- [ ] **Step 2: Create the line map**, and start the README revenue section (sources, frozen-file decision: *data stays out of git; the auxiliary copy rather than reading the published workbook, because the workbook's Revenue sheet will be regenerated from this pipeline*).
- [ ] **Step 3: Commit** `COL revenue: central line map, README design notes`

### Task 2: Central parser (Cuadros + frozen) in the extract notebook

**Files:**
- Create: `Colombia/extract_revenue_to_csv.py`: Databricks notebook source, `# MAGIC %run ../utils` cell, `IS_DATABRICKS` env pattern, reads `./central_revenue_lines.csv` relative to the notebook. Inputs are `{RAW_INPUT_DIR}/Colombia/central/revenues/*.xlsx` and `{RAW_INPUT_DIR}/Colombia/auxiliary/central_revenue_2019_2022.csv`, and outputs go to the CSV dir.
- Test: `tests/test_colombia_revenue_extract.py`; goldens `tests/fixtures/colombia/revenue_central_{2022,2024}.csv`

**Interfaces:**
- Produces:
  - `normalize_label(s: str) -> str`: lowercase, strip accents, collapse whitespace, drop a trailing footnote marker matching `\s+\d+/$`
  - `parse_cuadros(path: str, line_map: pd.DataFrame) -> pd.DataFrame` with columns `year, econ1, econ2, econ3, econ4, approved, executed, econ_rev, econ_rev_sub, exclude_from_total`. It raises `ValueError(f"Unmapped central revenue line in {sheet}: {label}")`, and `AssertionError` when the sum of non-subtotal lines ≠ the sheet's `TOTAL …` row (tolerance 1e6 pesos).
  - `tag_frozen(frozen: pd.DataFrame, line_map) -> pd.DataFrame` gives the same columns, categorised by `econ4` (lines with no category stay uncategorised; there's no failure for frozen rows).
  - Output file `revenue_central_{year}.csv` per year, both frozen and Cuadros years.
  - `exclude_from_total`: False for all central rows, except the C3 lines `recursos de credito interno` / `recursos de credito externo` when `EXCLUDE_CENTRAL_BORROWING` is True.

- [ ] **Step 1: Write failing tests**
  - `test_central_cuadros_golden`: build a fixture `CUADROS EJECUCION INGRESOS PGN DICIEMBRE 2024.xlsx` with `write_workbook`. Use sheets C1–C5 (C1 with a Nación block and an Establecimientos Públicos block) in the real layout (title rows, `Acumulado al mes de diciembre de 2024`, header `Concepto` row, values in cols 3/4) with three tax lines, one subtotal and a TOTAL row. Run the notebook, then compare `revenue_central_2024.csv` to its golden. Assert that the golden has the Establecimientos Públicos rows and no C1 Nación rows.
  - `test_unmapped_central_label_fails`: the same fixture plus a C2 line `Impuesto Especial para el Catatumbo`. `run_script` raises, and stderr contains `Unmapped central revenue line in C2: impuesto especial para el catatumbo`.
  - `test_central_units_and_columns`: in the golden, a source value of `1.5` in col 4 becomes `executed == 1.5e9`, col 3 becomes `approved`, and the 2025-style header `Vigente` / `Recaudo Neto` parses the same way.
  - `test_frozen_tagged`: from a fixture aux CSV containing OCR labels (`03 Recursos Oe Capital De La Nacion`, `14 Impuesto Naclonal Al Carbono`), `revenue_central_2022.csv` has the fixed labels, and `econ_rev == 'Excises'` / `econ_rev_sub == 'Excises (Fuels)'` on the carbon tax row.
- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement** the functions above. Get the year from regex `diciembre de (\d{4})`. Find the header row as the first row whose col 0 normalizes to `concepto`.
- [ ] **Step 4: Run** with `WRITE_GOLDEN=1` once, inspect the goldens by eye, then run without it → PASS
- [ ] **Step 5: Commit** `COL revenue: central extract`

### Task 3: Subnational parser (FUT 2019–2020, CUIPO 2021+)

**Files:**
- Modify: `Colombia/extract_revenue_to_csv.py` (subnational section)
- Test: `tests/test_colombia_revenue_extract.py`; goldens `tests/fixtures/colombia/revenue_subnational_{2020,2024}.csv`

**Interfaces:**
- Produces:
  - `read_subnational(year: int, ejecucion_path: str, programacion_path: str | None) -> pd.DataFrame` with columns `year, codigo_fut (str, 9 digits), entity_name, concept_code, econ1, econ2, econ3, econ4, approved, executed`. FUT years pass `programacion_path=None` (both measures come from one file). It locates the header row as the first row containing `codigo concepto` / `codigoconcepto` / `concepto_cod` after `normalize_label` with spaces and underscores removed. It also picks the sheet: `Raw` (2021 ejecución), `Ejecución de Ingresos` (2022 ejecución), `Programación de Ingresos` (2022 approved), otherwise the first sheet.
  - Entity key `codigo_fut`: 2021 `EntidadTerritorialID`; 2022 ejecución `nombreentidad[:9]`; 2022 approved `CodigoFut`; 2023+ and FUT `Código FUT`.
  - `leaf_rows(df) -> df`: drop rows whose `concept_code + '.'` is a prefix of another code for the same (`year`, `codigo_fut`), then sum the measures per (`year`, `codigo_fut`, `concept_code`). Drop FUT `VAL` rows and the root (`TI` / `1`) before this.
  - `econ1`–`econ3` = `"{code} {name}"` of the level-1/2/3 ancestor codes; `econ4` = `"{code} {name}"` of the leaf. Names come from the same file's rows.
  - Output: `revenue_subnational_{year}.csv` plus `revenue_subnational_root_{year}.csv` (`codigo_fut, approved, executed` from root rows, used by Task 5 checks).
  - Extract-time assertion: per entity, leaf sums = root row, within 1e3 pesos, for executed (and approved where present).

- [ ] **Step 1: Write failing tests**
  - `test_leaf_filter_and_repeat_sum`: a CUIPO fixture with codes `1`, `1.1`, `1.1.01`, `1.1.01.01.200` (twice, 100 and 50) and `1.1.01.02.200` (30) for one entity gives exactly two rows, executed 150 and 30.
  - `test_one_sided_concepts_kept`: concept `1.1.02.03.001` only in programación gives a row with `approved` set and `executed` NaN.
  - `test_cuipo_header_variants`: three fixtures with 2021-style (`Raw` sheet, `EntidadTerritorialID`, `TotalRecaudo`), 2022-style (`nombreentidad` "110505000 Antioquia", `totalRecaudo`) and 2024-style (17 title rows, `Código FUT`, `Total Recaudo`) layouts give identical frames apart from `year`.
  - `test_fut_measures`: in a FUT fixture, `executed` = `Total Ingresos` (not `Recaudo`), `approved` = `Presupuesto Definitivo`, and `VAL` rows are dropped.
  - `test_subnational_golden_2020_2024`: compare against the goldens.
- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement** the functions above.
- [ ] **Step 4: Run** → PASS (golden written once with `WRITE_GOLDEN=1`, then inspected)
- [ ] **Step 5: Commit** `COL revenue: subnational extract`

### Task 4: Subnational categories and total-revenue exclusions

**Files:**
- Modify: `Colombia/extract_revenue_to_csv.py`
- Test: `tests/test_colombia_revenue_extract.py`

**Interfaces:**
- Produces: `classify_subnational(code: str) -> tuple[str | None, str | None, bool]` returning (`econ_rev`, `econ_rev_sub`, `exclude_from_total`). It's applied to `revenue_subnational_{year}.csv` (adds the three columns). Rules match by code prefix (`code == p or code.startswith(p + '.')`):

| Category | CUIPO (2021+) | FUT (2019–2020) |
|---|---|---|
| `PREDIAL_CATEGORY` | `1.1.01.01.200` | `TI.A.1.3` |
| Excises / Excises (Fuels) | `1.1.01.02.109` | `TI.A.1.26` |
| Excises / Excises (Tobacco) | `1.1.01.02.106` | `TI.A.1.18` |
| Excises / Excises (Alcohol) | `1.1.01.02.103`, `.104`, `.105` | `TI.A.1.14`–`TI.A.1.17` |
| Permits and Fees | `1.1.01.01.100`, `1.1.01.02.300`, `1.1.02.02`, `1.1.02.03` | `TI.A.1.2`, `TI.A.1.28`, `TI.A.2.1`, `TI.A.2.2` |
| exclude_from_total = True | `1.1.02.06` (all current transfers incl. SGP `1.1.02.06.001`), `1.2.06`, `1.2.07`, `1.2.08`, `1.2.10`, `1.2.12` | `TI.A.2.6`, `TI.B.1`, `TI.B.4`, `TI.B.6`, `TI.B.10` |
| Subnational Own Revenues | every other non-excluded line (`econ_rev`, `econ_rev_sub` None) | same |

Excluded lines get `econ_rev = None`. Every other uncategorised line gets `econ_rev = 'Subnational Own Revenues'`, and categorised lines keep their category. (The workbook's own-revenue row sums all non-excluded Regional lines; see Task 6.)

- [ ] **Step 1: Write failing tests** `test_classify_subnational`, a table of `(code, expected)` covering every row above, plus: `1.1.01.01.2001` (not a child of `.200`) → Subnational Own Revenues; `TI.A.1.30` (not `TI.A.1.3`) → Subnational Own Revenues; with `PREDIAL_CATEGORY` patched to `('Income Tax', None)`, `1.1.01.01.200` → Income Tax.
- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement** with a prefix-table lookup that picks the longest match.
- [ ] **Step 4: Run** → PASS; regenerate the subnational goldens with `WRITE_GOLDEN=1` and check the diff only adds the three columns
- [ ] **Step 5: Commit** `COL revenue: subnational categories`

### Task 5: DLT transform and pipeline wiring

**Files:**
- Create: `Colombia/transform_load_revenue_dlt.py`
- Modify: `Colombia/extract_raw_microdata_excel_to_csv.py`: append a cell `# MAGIC %run ./extract_revenue_to_csv`, so the existing job task runs it (no DAG change)

**Interfaces:**
- Consumes: `revenue_central_*.csv`, `revenue_subnational_*.csv`, `revenue_subnational_root_*.csv` in `/Volumes/prd_mega/sboost4/vboost4/Workspace/raw_microdata_csv/Colombia`; `indicator_intermediate.col_subnational_adm2_adm1_lookup (adm1_name, adm2_name, nso_code)`
- Produces the tables `col_rev_central_bronze`, `col_rev_subnat_bronze`, `col_boost_rev_gold`, `col_rev_quality`. Gold columns, in order: `country_name, year, admin0, admin1, admin2, geo1, econ1, econ2, econ3, econ4, concept_code, econ_rev, econ_rev_sub, exclude_from_total, approved, executed`.
  - Subnational geography: `nso_code` from `codigo_fut` exactly as expenditure does (`Colombia/transform_load_raw_dlt.py:139-146`: last 5 digits; `%1000 == 0` → `/1000`). `admin1 = geo1 = adm1_name`, `admin2 = adm2_name`, `admin0 = 'Regional'`.
  - `col_rev_quality`: `@dlt.expect_or_fail` that per (year, codigo_fut) the gold Regional executed sum = root executed (abs diff < 1e3), and that no Regional row has null `admin1`.

- [ ] **Step 1: Run the extract on Databricks.** Upload the notebooks to `/Users/wlu4@worldbank.org/col_fix/Colombia/` and submit a one-time run of `extract_revenue_to_csv` on cluster `1111-165457-9ia3jd1w` (the same method as for PR #71). Expected: TERMINATED SUCCESS, with `revenue_central_{2019..2022,2024,2025}.csv` and `revenue_subnational_{2019..2024}.csv` in the CSV dir.
- [ ] **Step 2: Write `transform_load_revenue_dlt.py`.**
- [ ] **Step 3: Ask the user before changing the pipeline.** This is a prod config change: add `/Repos/boostprocessed/mega-boost/Colombia/transform_load_revenue_dlt` to pipeline `910c7eb9-8ec3-4d41-a2d8-052d98bb04f9` libraries. The Repos checkout must be on a branch that contains the notebook, so confirm with the user which branch to use.
- [ ] **Step 4: Full refresh and verify.** Expected: COMPLETED, and:
  - `select year, admin0, round(sum(executed)/1e12,2) from prd_mega.boost_intermediate.col_boost_rev_gold group by 1,2` gives Central 2019 237.43, 2020 288.54, 2021 320.71, 2022 329.86, 2024 400.91 (C1 TOTAL INGRESOS DEL PGN), 2025 482.50, and Regional for 2019–2024.
  - Department-government rows (codigo_fut ending `000`) have `admin2 IS NULL`.
- [ ] **Step 5: Finish the README** (pipeline layout, tables, quality checks, how to add a year) and commit `COL revenue: DLT transform`

### Task 6: Workbook shift report, then REV rows rewrite (after sign-off)

**Files (outside the repo, in the session scratchpad):** `rev_shift_report.py`, `update_rev_xlsx.py`

**Interfaces:**
- Consumes: `col_boost_rev_gold`, aggregated nationally per `(year, admin0, econ1, econ2, econ3, econ4, econ_rev, econ_rev_sub, exclude_from_total)`, plus `~/Downloads/Colombia BOOST.xlsx`.
- New REV formulas, for year columns P onward (2019+), on both sheets, using new named ranges `econ_rev_r` / `econ_rev_sub_r` / `excl_r` on the new `Revenue` columns:
  - row 299 (Total) = `SUMIFS(<measure>_r, year_r, P$1, excl_r, FALSE)`
  - row 305 (Subnational own) = the same plus `admin1_r, "Regional"`
  - category rows: `SUMIFS(<measure>_r, year_r, P$1, econ_rev_r, "<category>")`; sub rows use `econ_rev_sub_r`. Composite rows keep their structure (302 = 307 + 306; 306 = SUM(314:316))
  - rows shown as `..` today (308–310, 312–313, 317–320) stay `..`, except 319 Property Taxes (Immovable), which now has data.
- Pre-2019 Revenue rows and the 2006–2018 formulas stay unchanged. `Revenue` admin0 values `Local` become `Regional` only for the 2019+ rows (the pre-2019 formulas don't filter on admin0).

- [ ] **Step 1: Shift report.** For each REV row × 2019–2022, list old value, new value, difference, and the top lines driving the difference. Expected known shifts: Predial moves from row 300 to rows 311/319 in 2021–2022 (≈8.3/9.8 tn executed); row 300 gains nothing for 2019–2020. Send it to the user and **stop until they sign off**.
- [ ] **Step 2: After sign-off,** write the new `Revenue` rows and formulas through Excel (xlwings, sandbox folder `~/Library/Containers/com.microsoft.Excel/Data/`, `app.kill()` on the script's own instance only). Recalculate and save.
- [ ] **Step 3: Verify.** 2024 and 2025 REV rows are numeric. 2019–2022 values equal the "new" column of the signed-off report. 2006–2018 values are unchanged from the backup. There are no `#` error cells.
- [ ] **Step 4:** Put the result in `~/Downloads` (keep a dated backup) and tell the user. The OneDrive upload waits for their go-ahead.
