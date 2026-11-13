# Bangladesh: FY22+ from raw iBAS expenditure exports

## Why

Bangladesh currently comes from Massimo's hand-built `Bangladesh (total calculations).xlsx`
(2009–2022). New years should come straight from the Ministry of Finance's iBAS exports so that
FY25 onward is "drop in the new `EXP_*` file and rerun", with Massimo maintaining only the lookups.

## Goals

- Build `bgd_boost_gold` years 2022+ from `EXP_2021_22`, `EXP_2022_23`, `EXP_2023_24` and any
  later `EXP_YYYY_YY.xlsx` of the same shape, with no code change per new year.
- Classify rows with Massimo's lookups read directly from his `Keys` workbook.
- Apply the CCI 2019-onward formula set (already mirrored in `bgd_2019_onward_boost_silver`).
- Fail loudly, listing the values, when an export holds codes the lookups don't cover.

## Non-goals

- Approved / revised for 2022+: the exports carry `EXECUTED` only. They stay null until a MoF
  budget source exists.
- Moving Bangladesh off DLT (separate later change, Togo pattern).
- Rebuilding 2014–2021 from raw; FY15–18 and FY21 raw exports don't exist in our hands.
- New gold columns (e.g. foreign funding from `FUND`, rigidity, WB geo codes).

## Anti-goals

- Writing anything back to SharePoint / `Auxiliary` (it is a sync destination).
- Silently dropping or guessing rows with unmapped codes.

## Inputs (all via the BOOSTProcessed → volume sync)

| Input | Volume path |
|---|---|
| iBAS exports | `Documents/input/Data from authorities/Bangladesh/other/EXP_*.xlsx` (already there) |
| Massimo's lookups | `Documents/input/Auxiliary/Bangladesh Keys 23-25.xlsx` (to copy in) |

Export shape: one sheet, 2 title rows (`FISCAL YEAR : 2021-22`, `(Amount in Taka)`), header on
row 3: `ADMIN1 ADMIN2 ADMIN3 ADMIN4 GEO1 GEO2 GEO3 BUDGETTYPE ECON1 ECON4 ECON6 FUND EXECUTED`.
Year = end year of the fiscal-year title (2021-22 → 2022), matching existing gold.

Lookup sheets used (read by name; only these sheets are parsed, the big FYxx data sheets are skipped):

| Sheet | Key | Gives | Checked against the 3 exports |
|---|---|---|---|
| `ECON` | 7-digit code of `ECON6` | `ECON 1`..`ECON 5` labels | 517 codes, no conflicts; 0–5 codes per year missing |
| `BT-ADM` | 3-digit ministry code (col A) | `ADMIN2_STANDARDIZED`, `FUNC1`, `FUNC_GROUP` | 62 codes, no conflicts; exports also hold 5-digit `ADMIN2` units |

`GEO-BT` and `Rigidity` are not needed for gold columns and are not read (start simple).

## Approaches considered

- **A (picked): extend the existing DLT pipeline.** New pandas extract notebook; new
  `bgd_2022_onward` bronze/silver in `BGD_transform_load_dlt.py`; gold unions it. Smallest change;
  the job and pipeline config stay as they are.
- B: convert Bangladesh to the Togo plain-notebook pattern. Matches the no-DLT direction but
  doubles scope and rewrites stable 2009–2021 code; deferred.
- C: separate plain notebook for 2022+ feeding the DLT gold. Rejected: cross-job ordering.

Gold integration (picked): old pipeline keeps 2009–2021, the EXP path supplies 2022+, gold unions
them. 2022 moves from the workbook to the raw export and loses its approved figure.

## Design

### Extract — `Bangladesh/BGD_extract_raw_microdata_excel_to_csv.py` (pandas, Databricks notebook source)

1. Read the `ECON` and `BT-ADM` sheets from the Keys workbook into code-keyed lookups.
2. For each `EXP_*.xlsx`: read with header on row 3, drop blank rows, derive `year` from the title.
3. Join:
   - `ECON6` 7-digit code → `ECON 1..5`. If the code is not in `ECON`, fall back to any `ECON`
     row with the same 5-digit prefix (the 5-digit code is the parent `ECON 5`, and every level
     above is implied by it). Report how many rows/amount used the fallback.
   - `ADMIN2` code → first 3 digits → `BT-ADM`. Keep the raw `ADMIN2` (trimmed) too: the CCI
     rules name 5-digit units (`14503-Department of Environment`, `12402-Directorate of Primary
     Education`, railway/telecom units).
4. Any code still unmapped after the fallback → raise with the distinct values and their
   executed totals.
5. Write `Workspace/raw_microdata_csv/Bangladesh/<year>.csv` (skip if it exists, as today).

### Transform — `bgd_2022_onward_boost_bronze` / `_silver` in `BGD_transform_load_dlt.py`

Same rules as `bgd_2019_onward_boost_silver`, matching econ labels by numeric code prefix
(exports mix `3-Recurrent Expenditure` and `3 - Recurrent Expenditure`):

- keep `ECON 1` codes 3 and 4 (drops 7 Assets, 8 Liabilities, revenue)
- `func` from `FUNC_GROUP`/`FUNC1` + raw `ADMIN2` (environment), `func_sub` from raw `ADMIN2`
- `econ` / `econ_sub` from `ECON 1..5`; **Basic Services** from the 7-digit `ECON6` prefixes
  `3211113/14/15/20/22/29` (the CCI's `Old_ECON3` is this same 7-digit code)
- `geo1` from `GEO1` with the numeric prefix stripped (`30-Dhaka` → `Dhaka`)
- `admin0`: see open question 1
- `approved`, `revised` = null

`bgd_2019_onward_boost_bronze` is limited to 2019–2021; gold unions the new silver.

### Checks

- Unit/integration test (`tests/test_bangladesh_extract.py`, unittest, fixture workbooks built in
  code like `test_albania_extract.py`): year parsing, both lookups, 5-digit admin fallback,
  7-digit econ fallback, unmapped-code failure.
- One-off reconciliation (local, not CI): extract output vs Massimo's keyed `FY22`–`FY24.xlsx` —
  executed totals by `ECON 2`, `FUNC1`, `ADMIN2_STANDARDIZED` per year. Report differences.
- After deploy: compare 2022 gold vs the CCI Executed column for 2022 (total, wage bill, capex,
  goods & services, functions).

## Open questions

1. **Central vs regional (`admin0`).** The CCI's 2019+ subnational formulas test `admin0` for
   `*CAFO`/`*DAFO`, but `ADMIN0` is empty in the 2019+ sheets, so today's 2019–2021 gold is all
   "Central". The exports have the office in `ADMIN1` (`CAFO …` ministry finance offices,
   `DAFO …`/`UAO …`/`DCA …` field offices, plus many self-accounting entities). Proposal:
   Regional = `ADMIN1` starts with `DAFO`, `UAO`, `DCA`, `DAO`; everything else Central (same
   meaning as the 2015–18 `DAO`/`UAO` rule). To confirm with Massimo.
2. Budget source for FY22+ (Massimo).

## Left to the builder

Column naming inside the CSV, how the fallback report is printed, fixture contents.
