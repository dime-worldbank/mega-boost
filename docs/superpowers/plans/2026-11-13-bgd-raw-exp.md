# Bangladesh FY22+ from raw iBAS exports — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `bgd_boost_gold` gets 2022+ from the MoF `EXP_YYYY_YY.xlsx` exports, classified with Massimo's `Keys` lookups and the CCI 2019+ rules; the CCI-vs-pipeline differences are recorded.

**Architecture:** A new pandas extract notebook joins each export with the `ECON` and `BT-ADM` sheets of the Keys workbook and writes one CSV per year. The existing extract notebook `%run`s it, so the `BOOST Harmonize UC` job needs no change. The existing DLT pipeline gains `bgd_2022_onward` bronze/silver; gold unions it, and the 2019-onward tables stop at 2021.

**Tech Stack:** pandas 1.5.3 + openpyxl (extract, runs locally and on Databricks), PySpark DLT (transform), unittest.

**Spec:** `docs/superpowers/specs/2026-11-13-bgd-raw-exp-design.md`

## Global Constraints

- Inputs on the volume: exports `{RAW_INPUT_DIR}/Bangladesh/other/EXP_*.xlsx`; Keys `{TOP_DIR}/Documents/input/Auxiliary/Bangladesh Keys 23-25.xlsx`.
- Output CSVs: `{WORKSPACE_DIR}/raw_microdata_csv/Bangladesh/<year>.csv` (via `prepare_raw_microdata_csv_dir`). A year is (re)written only when its CSV is missing or older (mtime) than the Keys file or that year's export; otherwise it is skipped with a printed message.
- Year = end year of the export's title row (`FISCAL YEAR : 2021-22` → 2022; match case-insensitively).
- Never write to SharePoint/OneDrive or `Auxiliary`.
- Codes are matched on their leading digits, never on labels (`3-Recurrent Expenditure` vs `3 - Recurrent Expenditure`; `ADMIN2` values carry trailing spaces/newlines).
- Keys sheets are read by name: `ECON` (col A = 7-digit `ECON 6` label, cols B–F = `ECON 1`..`ECON 5` labels) and `BT-ADM` (header on row 2; col A = 3-digit ministry code, C = `ADMIN2_STANDARDIZED`, D = `FUNC1`, E = `FUNC_GROUP`). Only those two sheets are parsed (openpyxl `read_only`).
- Python follows `~/.claude/rules/python.md`: imports at top, why-comments only.
- Commits: short subject, `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, no Claude-Session line; commit locally, ask before any push.

## Review Focus

1. Title-row variants (`FISCAL YEAR : 2021-22`, `Fiscal Year : 2022-23`) → both parse; a title with no `YYYY-YY` fails naming the file. (Task 1 test)
2. `ADMIN2` like `"101-President's Office\n"` and 5-digit units (`11301-Secretariat, ERD`) → map through the first 3 digits. (Task 1 test)
3. `ECON6` 7-digit code absent from `ECON` but its 5-digit parent present → fallback mapping, counted in the printed report; absent at both levels → the run fails listing the code and its executed total. (Task 1 tests)
4. Blank rows and rows without `EXECUTED` in the export → dropped, not mapped. (Task 1 test)
5. Two export files resolving to the same year → fail rather than silently overwrite. (Task 1 test)

---

### Task 1: Extract notebook with tests

**Files:**
- Create: `Bangladesh/BGD_extract_raw_microdata_excel_to_csv.py`
- Modify: `Bangladesh/BGD_extract_microdata_excel_to_csv.py` (append a cell `# MAGIC %run ./BGD_extract_raw_microdata_excel_to_csv`)
- Test: `tests/test_bangladesh_extract.py`

**Interfaces:**
- Produces (CSV contract read by Task 2), one file per year, columns in this order:
  `year, admin1, admin2, admin3, admin4, geo1, geo2, geo3, budget_type, fund, econ1, econ2, econ3, econ4, econ5, econ6, admin2_standardized, func1, func_group, executed`
  — `admin*`, `geo*`, `budget_type`, `fund`, `econ6`, `executed` are the export's values (strings stripped); `econ1`..`econ5` are the `ECON` sheet labels; `admin2_standardized`, `func1`, `func_group` are the `BT-ADM` values.
- Notebook header pattern copied from `Albania/ALB_extract_raw_microdata_excel_to_csv.py`: `IS_DATABRICKS`; on Databricks use `RAW_INPUT_DIR`, `TOP_DIR`, `prepare_raw_microdata_csv_dir` from `%run ../utils`; locally read env `RAW_INPUT_DIR` (dir containing `Bangladesh/other/`), `KEYS_FILE`, `OUTPUT_DIR`.
- Functions (module level, used only inside the notebook):
  - `leading_code(value) -> str | None` — leading digit run of a cell, else None.
  - `read_keys(path) -> tuple[dict, dict]` — `(econ_by_code7, admin_by_code3)`; `econ_by_code7[code7] = (econ1..econ5 labels)`, `admin_by_code3[code3] = (admin2_standardized, func1, func_group)`; first occurrence wins (both sheets verified conflict-free).
  - `read_export(path) -> tuple[int, pd.DataFrame]` — year from row 1, header on row 3, rows without `EXECUTED` dropped.
  - `is_stale(csv_path, *input_paths) -> bool` — True when `csv_path` is missing or its mtime is older than any input's.
  - `classify(df, econ_by_code7, admin_by_code3) -> pd.DataFrame` — adds the Keys columns; 5-digit fallback for econ (any `econ_by_code7` entry whose code starts with the row's first 5 digits); prints rows/amount that used the fallback; raises `ValueError` listing every unmapped econ6 / admin2 value with its executed total.

- [ ] **Step 1: Write the failing tests** in `tests/test_bangladesh_extract.py`, building fixtures with `helpers.write_workbook` and running the notebook with `helpers.run_script` (env `RAW_INPUT_DIR`, `KEYS_FILE`, `OUTPUT_DIR`). Fixture Keys: `ECON` rows for `3111101-Basic pay (Officer)` → (`3 - Recurrent Expenditure`, `31 - Compensation of Employees`, `311 - Wages and Salaries`, `3111 - Wages and salaries in cash`, `31111 - Officers' pay`) and `3211113-Electricity` → (`3 - Recurrent Expenditure`, `32 - Purchases of Goods and Services`, `321 - Administrative Expenses`, `3211 - Administrative expenses`, `32111 - Administrative Expenses`); `BT-ADM` title row, header row, then `101 | 101-President's Office | 101-President's Office | Public Services | Public Sector` and `113 | 113-Economic Relations Division | 113-Economic Relations Division | Public Services | Public Sector`. Exports: two title rows + the 13-column header.
  - `test_writes_one_csv_per_year_with_contract_columns`: `EXP_2021_22.xlsx` (title `FISCAL YEAR : 2021-22`) and `EXP_2022_23.xlsx` (title `Fiscal Year : 2022-23`) → `2022.csv`, `2023.csv`; columns equal the contract list; `year` column all 2022 / 2023.
  - `test_admin2_maps_by_three_digit_code`: rows with `ADMIN2` `"101-President's Office\n"` and `11301-Secretariat, Economic Relations Division (ERD)` → `func1 == 'Public Services'`, `admin2_standardized` `101-President's Office` / `113-Economic Relations Division`; `admin2` keeps the raw value stripped.
  - `test_econ_falls_back_to_five_digit_parent`: `ECON6 = 3111116-Arrear pay` (not in `ECON`) → `econ5 == "31111 - Officers' pay"`, `econ1 == '3 - Recurrent Expenditure'`.
  - `test_unmapped_codes_fail_with_list`: an export row with `ECON6 = 9999999-Unknown` and one with `ADMIN2 = 999-Nowhere` → `run_script` raises `AssertionError` whose message contains `9999999` and `999-Nowhere`.
  - `test_blank_and_no_executed_rows_dropped`: a fully blank row and a row with `EXECUTED` None → absent from the CSV (row count check).
  - `test_duplicate_year_fails`: `EXP_2021_22.xlsx` and `EXP_2021_22 copy.xlsx` both titled 2021-22 → failure mentioning `2022`.
  - `test_title_without_year_fails`: title `FISCAL YEAR :` → failure naming the file.
  - `test_skips_when_csv_newer_than_inputs`: run once; set the CSV mtime to the future with `os.utime`; replace the export's `EXECUTED` value; run again → CSV content unchanged.
  - `test_rewrites_when_keys_newer`: run once; set the CSV mtime to the past, change `func1` for `101` in the Keys fixture; run again → CSV shows the new `func1`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd tests && python -m unittest test_bangladesh_extract -v`
Expected: FAIL/ERROR (notebook file does not exist).

- [ ] **Step 3: Implement the notebook** with the functions above. Find exports with `glob(f"{RAW_INPUT_DIR}/Bangladesh/other/EXP_*.xlsx")`. Read the export with `pd.read_excel(path, header=2, dtype=str)` then `pd.to_numeric` on `EXECUTED`; read the title with openpyxl `read_only` (cell A1). Read the Keys file only if some year is stale. For each export, find the year from the title, then skip unless `is_stale(csv, KEYS_FILE, export)`; write with `df.to_csv(index=False, encoding='utf-8')`. Append the `%run ./BGD_extract_raw_microdata_excel_to_csv` cell to `BGD_extract_microdata_excel_to_csv.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd tests && python -m unittest test_bangladesh_extract -v`
Expected: 9 tests OK. Then the full suite `python -m unittest` still passes.

- [ ] **Step 5: Run against the real files locally** (not committed)

Run (from `Bangladesh/`): `RAW_INPUT_DIR=".../Documents - BOOSTProcessed/input/Data from authorities" KEYS_FILE=".../Documents - BOOSTProcessed/input/Auxiliary/Bangladesh Keys 23-25.xlsx" OUTPUT_DIR=<scratchpad>/bgd_csv python BGD_extract_raw_microdata_excel_to_csv.py`
Expected: `2022.csv`, `2023.csv`, `2024.csv`; fallback report shows 5 / 0 / 4 econ codes; no unmapped failure; row counts 851,791 / 834,584 / 904,757.

- [ ] **Step 6: Commit**

```bash
git add Bangladesh/BGD_extract_raw_microdata_excel_to_csv.py Bangladesh/BGD_extract_microdata_excel_to_csv.py tests/test_bangladesh_extract.py
git commit -m "BGD: extract FY22+ from raw iBAS exports with Keys lookups"
```

---

### Task 2: 2022-onward bronze/silver and gold union

**Files:**
- Modify: `Bangladesh/BGD_transform_load_dlt.py` — 2019-onward bronze file list; new tables after the 2019-onward silver; gold union.

**Interfaces:**
- Consumes: the Task 1 CSV contract from `f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'`.
- Produces: `bgd_2022_onward_boost_bronze`, `bgd_2022_onward_boost_silver` with the gold columns `year, BUDGET, REVISED, EXECUTED, admin0, admin1, admin2, geo1, func, func_sub, econ, econ_sub` (`BUDGET`, `REVISED` null doubles).

Silver rules (CCI 2019+ formula set; `code(c)` = leading digits of column `c`):

| Column | Rule |
|---|---|
| filter | `code(econ1)` in `3`, `4` |
| admin0 | `Regional` if `admin1` starts with `CAFO` or `DAFO`, else `Central` |
| geo1 | `geo1` with leading `\d+\s*-?\s*` stripped when it starts with digits, else `Central Scope` |
| admin1 | `Central Scope` if admin0 Central, else geo1 |
| admin2 | `admin2_standardized` |
| func | `func1`: Defence Services→Defence; Public Order and Safety→Public order and safety; Housing→Housing and community amenities; Health→Health; Recreation, Culture and Religious Affairs→Recreation, culture and religion; Education and Technology→Education; Social Security and Welfare→Social protection; then `code(admin2)` in `145`, `14503`→Environmental protection; then `func_group == 'Economic Sector'` and `func1` not in Housing / Local Government and Rural Development→Economic affairs; else General public services (same order as `bgd_2019_onward_boost_silver`) |
| func_sub | Public order and safety: `code(admin2)` starts with `122`→Public Safety, else Judiciary. Education: starts with `124`→Primary Education, `125`→Secondary Education |
| econ_sub | `code(econ3)=='311'`: `code(econ5)=='31113'`→Allowances else Basic Wages; `code(econ6)[:7]` in 3211113, 3211114, 3211115, 3211120, 3211122, 3211129→Basic Services; `code(econ4)=='3257'`→Employment Contracts; `code(econ3)=='372'`→Social Assistance; `code(econ5)=='37311'`→Pensions |
| econ | `code(econ1)=='4'`→Capital expenditures; `code(econ2)`: 31→Wage bill, 32→Goods and services, 34→Interest on debt, 35→Subsidies; econ_sub in Social Assistance/Pensions→Social benefits; else Other expenses |

(Matching ministries on the 3-digit code is equivalent to the CCI's exact-name lists: in all three exports the 122/124/125/145 ministries appear only under their 3-digit labels.)

- [ ] **Step 1: Limit `boost_2019_onward_bronze`** to `[f"{COUNTRY_MICRODATA_DIR}/{y}.csv" for y in (2019, 2020, 2021)]` and drop its stale TODO.
- [ ] **Step 2: Add `bgd_2022_onward_boost_bronze`** reading `f"{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}/*.csv"` with `CSV_READ_OPTIONS`, `inferSchema`, `year` cast to int, `@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")`.
- [ ] **Step 3: Add `bgd_2022_onward_boost_silver`** implementing the table above.
- [ ] **Step 4: Union it in `boost_gold`** after the 2019-onward silver.
- [ ] **Step 5: Syntax check** — `python -c "import ast,sys; ast.parse(open('Bangladesh/BGD_transform_load_dlt.py').read())"` → no output.
- [ ] **Step 6: Commit** — `git commit -m "BGD: 2022-onward silver from raw exports; 2019-onward stops at 2021"`

---

### Task 3: Databricks dev run and reconciliation

Needs the user's go-ahead before pushing the branch or creating the pipeline.

- [ ] **Step 1: Push `bgd-raw-exp`** (after asking) and check it out in a workspace Git folder `/Users/wlu4@worldbank.org/bgd_raw/` (user action or `databricks repos`).
- [ ] **Step 2: Run the extract** from that Git folder: notebook `Bangladesh/BGD_extract_microdata_excel_to_csv` (it `%run`s the new one). Expected: `/Volumes/prd_mega/sboost4/vboost4/Workspace/raw_microdata_csv/Bangladesh/{2022,2023,2024}.csv`.
- [ ] **Step 3: Create `TEMP bgd raw dev (wlu4) - delete after PR merges`**: copy of `BOOST Bangladesh TL UC` with library `/Users/wlu4@worldbank.org/bgd_raw/Bangladesh/BGD_transform_load_dlt`, target `boost_dev`, development mode; run a full refresh. Expected: success, `prd_mega.boost_dev.bgd_boost_gold` has years 2009–2024.
- [ ] **Step 4: Reconcile** with SQL on `boost_dev.bgd_boost_gold` (via `databricks api post /api/2.0/sql/statements`), per year:
  - 2022: total, each `econ`, `econ_sub`, `func`, `func_sub` and Regional total vs the CCI Executed 2022 column of `Bangladesh (total calculations).xlsx`. Expected to match within ~1% except the documented items (capex +3.3%, basic services +6%, allowances +1.4%, and the BT-ADM corrections on judiciary / public order / health / recreation / economic relations).
  - 2022–2024: gold executed total per year == sum of `executed` in the Task 1 CSVs where `econ1` code is 3 or 4.
  - 2019–2021: gold unchanged vs `prd_mega.boost_intermediate.bgd_boost_gold` (row count and executed sum per year).
  - Break down the capex and basic services gaps against Massimo's keyed `Raw data/FY22.xlsx` (sum by `ECON 2`/`ECON 4`) and note the cause.

---

### Task 4: Record differences and update docs

**Files:**
- Create: `Bangladesh/README.md`
- Modify: `docs/superpowers/specs/2026-11-13-bgd-raw-exp-design.md` (CSV rewritten only when older than Keys/export; extract wired via `%run`)

- [ ] **Step 1: Write `Bangladesh/README.md`** with three sections:
  - **Sources and update steps:** which years come from which input; for a new year, drop `EXP_YYYY_YY.xlsx` into `BOOSTProcessed/input/Data from authorities/Bangladesh/other/`, refresh `Auxiliary/Bangladesh Keys 23-25.xlsx` when Massimo updates it, rerun the job; what an unmapped-code failure means and that the codes go back to Massimo.
  - **Classification rules:** the silver table from Task 2 in brief, with the CCI line each rule mirrors.
  - **Differences from the CCI:** the Task 3 table (line, pipeline, CCI, diff, cause), the three BT-ADM corrections, the export having no liabilities rows, approved/revised null for 2022+, and the CAFO/UAO subnational question for Massimo.
- [ ] **Step 2: Update the spec** lines that changed.
- [ ] **Step 3: Commit** — `git commit -m "BGD: README with update steps and CCI differences"`
- [ ] **Step 4: Ask before pushing**; on approval push and open the PR. Before merge, `git rm -r docs/superpowers` (per the source-registry convention) and delete the TEMP pipeline after merge.
