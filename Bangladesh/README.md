# Bangladesh

## Sources

| Years | Input | Notebook |
|---|---|---|
| 2009–2021 | `Documents/input/Countries Special/Bangladesh (total calculations).xlsx`, one sheet per year, BOOST-coded by the country team | `BGD_extract_microdata_excel_to_csv.py` → `microdata_csv/Bangladesh/<year>.csv` |
| 2022 onward | iBAS expenditure exports from the Ministry of Finance, `Documents/input/Data from authorities/Bangladesh/other/EXP_YYYY_YY.xlsx` | `BGD_extract_raw_microdata_excel_to_csv.py` → `raw_microdata_csv/Bangladesh/<year>.csv` |

The raw exports carry **executed amounts only**, so `approved` and `revised` are empty from 2022.

The exports are classified with the lookups the country team (Massimo Mastruzzi) maintains in
`Documents/input/Auxiliary/Bangladesh Keys 23-25.xlsx`. Only two of its sheets are used:

- `ECON`: 7-digit `ECON6` code → `ECON 1`..`ECON 5`. A 7-digit code missing from the sheet takes
  the labels of its 5-digit parent; the extract prints which codes did.
- `BT-ADM`: 3-digit ministry code → `ADMIN2_STANDARDIZED`, `FUNC1`, `FUNC_GROUP`. 5-digit units
  (e.g. `11301-Secretariat, ERD`) map through their first 3 digits.

`BGD_extract_microdata_excel_to_csv.py` runs the raw extract with `%run`, so the
`BOOST Harmonize UC` job's `bgd_extract` task covers both.

## Adding a year

1. Copy the new `EXP_YYYY_YY.xlsx` into `Documents - BOOSTProcessed/input/Data from authorities/Bangladesh/other/`
   (OneDrive; it syncs to the volume).
2. If the country team updated their Keys workbook, copy it over
   `Documents - BOOSTProcessed/input/Auxiliary/Bangladesh Keys 23-25.xlsx` (keep the file name,
   or change `KEYS_FILE` in the extract).
3. Rerun the job. A year's CSV is rewritten only when it is missing or older than its export or the
   Keys file.

If the extract fails with **unmapped codes**, the export has `ECON6` or `ADMIN2` codes the Keys
workbook doesn't cover yet. The error lists each code with its executed total; send them to the
country team to add to the Keys workbook, then repeat step 2.

## Classification (2022 onward)

`bgd_2022_onward_boost_silver` applies the CCI's 2019-onward formulas, matching codes by their
leading digits:

| Column | Rule | CCI line |
|---|---|---|
| filter | `ECON 1` 3 (recurrent) or 4 (capital); drops 7 assets, 8 liabilities | Total expenditures |
| admin0 | Regional when `ADMIN1` starts with `CAFO` or `DAFO` | Subnational (`admin0` `*CAFO`, `*DAFO`) |
| func | from `FUNC1` / `FUNC_GROUP`; ministry 145 / 14503 → Environmental protection | functional lines |
| func_sub | ministry 122 → Public Safety, rest of public order → Judiciary; 124 → Primary, 125 → Secondary Education | judiciary, public safety, primary/secondary education |
| econ | `ECON 1` 4 → Capital; `ECON 2` 31 Wage bill, 32 Goods and services, 34 Interest, 35 Subsidies; Social Assistance/Pensions → Social benefits; rest Other | economic lines |
| econ_sub | `ECON 3` 311 → Basic Wages / Allowances (`ECON 5` 31113); `ECON6` 3211113/14/15/20/22/29 → Basic Services; `ECON 4` 3257 → Employment Contracts; `ECON 3` 372 → Social Assistance; `ECON 5` 37311 → Pensions | sub-economic lines (the CCI's `Old_ECON3` is the 7-digit `ECON6`) |
