# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %md
# MAGIC # Central African Republic BOOST expenditures: DGB execution statement to the BOOST microdata
# MAGIC The Direction Générale du Budget delivers the SIM_ba "Etat de développement des dépenses" as one workbook with a
# MAGIC sheet per year (`2022`, `2023`, ...), amounts in thousands of FCFA. Each sheet nests chapter header rows
# MAGIC (`AA : label`), service header rows (`SSSSSS : label`) and coded lines `AA-PPPPPP-SSSSSS-T-EEEE-FF : label`.
# MAGIC This notebook dissects the code into the columns of the `Expenditure` sheet of the delivered
# MAGIC `Central African Republic BOOST.xlsx`, as the `intermediate files/2022 conversion to BOOST.xlsx` recipe does, and
# MAGIC writes one `YYYY.csv` per year to `raw_microdata_csv/Central African Republic`, in the sheet's row order and with
# MAGIC its columns plus the raw `Code`.
# MAGIC
# MAGIC | block | column | label |
# MAGIC |---|---|---|
# MAGIC | AA chapter | ADMIN1 | the sheet's chapter header row |
# MAGIC | PPPPPP programme/action | ADMIN2 | the code, plus the label of the sheet's service header with the same code where there is one |
# MAGIC | SSSSSS service | kept in `Code` only | |
# MAGIC | T titre | ECON1T | `CAF_econ_mapping.json` (equals the titre digit) |
# MAGIC | EEEE compte | ECON2T, ECON3T | `CAF_econ_mapping.json`; ECON4T = compte + the line's own label |
# MAGIC
# MAGIC `CAF_econ_mapping.json` (compte → econ1-econ3 labels, the economic block of the 2022 conversion's bridge table) is
# MAGIC kept on the volume under `Documents/input/Auxiliary`, next to Albania's project labels, not in the repo.
# MAGIC | FF source | not carried over, as in the 2022 conversion | |
# MAGIC
# MAGIC FUNCTION1, SOURCE_FIN, Source2, road_m and road are empty for these years, as in the sheet. Amounts, x1000:
# MAGIC Dotation = Total prévisions, Engagé = 2. Engagements, Liquidé = 3. Dépenses liquidées, Ordonnancé = 4. Ordon. en N,
# MAGIC Ord. PEC = Paiements sur les ordon. de N.
# MAGIC
# MAGIC Only 2023 onward is processed: 2022 is the SIM_ba transition year and is left empty on purpose, and 2016-2021 stay
# MAGIC as delivered in the workbook. Donor-funded capital spending is not in the statement and is still to come from the
# MAGIC authorities.
# MAGIC
# MAGIC ADMIN1 and ADMIN2 reproduce the delivered 2023-2025 rows, as agreed with the manager on 2026-11-09: ADMIN2 is the
# MAGIC PPPPPP block, labelled by the same year's service header carrying that code when there is one (a code heading
# MAGIC several services takes the label of the last header, as the workbook's lookup does) and left bare otherwise; the
# MAGIC programme classification is abandoned, so PROGRAM 1 stays empty where the workbook carries the SSSSSS block.
# MAGIC Two other differences: the workbook's bare ADMIN2 codes end with a space, dropped here, and the one 2024 line on
# MAGIC compte 2452 shows `#N/A` in ECON2T/ECON3T there and is empty here. None of this feeds the `Approved`/`Executed`
# MAGIC indicator formulas.

# COMMAND ----------

import json
import os
import re
from glob import glob
from pathlib import Path

import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
COUNTRY = 'Central African Republic'
if IS_DATABRICKS:
    # TOP_DIR, RAW_INPUT_DIR and prepare_raw_microdata_csv_dir come from %run ../utils
    raw_microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)
    econ_mapping_file = f"{TOP_DIR}/Documents/input/Auxiliary/CAF_econ_mapping.json"
else:
    RAW_INPUT_DIR = os.environ.get('RAW_INPUT_DIR') or input(f"Enter the raw input directory (containing {COUNTRY}/ with the DGB workbook): ").strip()
    raw_microdata_csv_dir = os.environ.get('OUTPUT_DIR') or input("Enter the output directory: ").strip()
    econ_mapping_file = os.environ.get('ECON_MAPPING_FILE') or input("Enter the path to CAF_econ_mapping.json: ").strip()
    Path(raw_microdata_csv_dir).mkdir(parents=True, exist_ok=True)

# 2022 is the SIM_ba transition year and is left empty on purpose; earlier years stay as delivered in BOOST 2.0.xlsx
RAW_DATA_START_YEAR = 2023

# compte (4 digits) -> econ1, econ2, econ3 labels: the economic block (columns A:E) of the 2022 conversion's bridge
# table, first row per compte as its VLOOKUPs return it. Compte 2452 has no econ2/econ3 there.
with open(econ_mapping_file, encoding='utf-8') as json_file:
    econ_mapping = json.load(json_file)

# a few 2024-25 lines carry a 5-digit compte (60333) or a 1-digit source
CODE = re.compile(r"^(\d{2})-(\d{6})-(\d{6})-(\d)-(\d{4,5})-(\d{1,2})\s*:\s*(.*)$")
STANDARD_CODE = r"\d{2}-\d{6}-\d{6}-\d-\d{4}-\d{2}"
CHAPTER = re.compile(r"^(\d{2}) : (.*)$")
SERVICE = re.compile(r"^(\d{6}) : (.*)$")

# BOOST column -> offset from the 'Imputation' column of the statement, and the header text expected there
AMOUNT_COLUMNS = {'Dotation': 3, 'Engagé': 5, 'Liquidé': 8, 'Ordonnancé': 10, 'Ord. PEC': 13}
EXPECTED_HEADERS = {3: 'Total', 5: '2. Engagements', 8: '3. Dépenses liquidées', 10: '4. Ordon. en N', 13: 'Sur les ordon. de N'}
# the columns of the Expenditure sheet, plus the raw code
COLUMNS = ['YEAR', 'ADMIN1', 'PROGRAM 1', 'ADMIN2', 'FUNCTION1', 'ECON1T', 'ECON2T', 'ECON3T', 'ECON4T', 'SOURCE_FIN',
           'Source2', 'Dotation', 'Engagé', 'Liquidé', 'Ordonnancé', 'Ord. PEC', 'road_m', 'road', 'Code']

# COMMAND ----------

workbooks = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/*.xlsx')
assert len(workbooks) == 1, f"expect one DGB workbook under {RAW_INPUT_DIR}/{COUNTRY}, found {len(workbooks)}"
workbook = workbooks[0]
years = sorted(int(sheet) for sheet in pd.ExcelFile(workbook).sheet_names
               if re.fullmatch(r'\d{4}', sheet) and int(sheet) >= RAW_DATA_START_YEAR)
assert years, f"No year sheets >= {RAW_DATA_START_YEAR} in {workbook}"
print("years:", ", ".join(map(str, years)))

# COMMAND ----------

def read_statement(year):
    """Rows below the 'Imputation' header of a year's sheet, after checking the amount columns sit where expected."""
    df = pd.read_excel(workbook, sheet_name=str(year), header=None)
    header_idx = df.index[df[0] == 'Imputation'][0]
    for offset, expected in EXPECTED_HEADERS.items():
        got = ' '.join(str(df.iat[header_idx, offset]).split())
        assert got == expected, f"{year}: column {offset} reads {got!r}, expected {expected!r}"
    return df.iloc[header_idx + 1:]

def header_labels(df):
    """{chapter: 'AA label'} and {service code: label} from the sheet's header rows; a service code printed under several
    chapters keeps the label of its last header, as the delivered workbook's lookup does."""
    chapters, services = {}, {}
    for cell in df[0]:
        if not isinstance(cell, str):
            continue
        if m := CHAPTER.match(cell):
            chapters[m[1]] = f"{m[1]} {m[2].strip()}"
        elif m := SERVICE.match(cell):
            services[m[1]] = m[2].strip()
    return chapters, services

def text(value):
    return None if value is None or pd.isna(value) else str(value)

def convert(df, year, chapters, services):
    rows = []
    for _, row in df.iterrows():
        m = CODE.match(row[0]) if isinstance(row[0], str) else None
        if not m:
            continue  # header, total and footer rows
        chapter, programme, _service, titre, compte, _source, label = m.groups()
        assert chapter in chapters, f"{year}: no chapter header for {row[0]!r}"
        econ = econ_mapping.get(compte[:4])
        assert econ, f"{year}: compte {compte} missing from CAF_econ_mapping.json ({row[0]!r})"
        assert econ['econ1'][:1] == titre, f"{year}: titre {titre} disagrees with {econ['econ1']!r} ({row[0]!r})"
        amounts = {name: pd.to_numeric(row[offset], errors='coerce') for name, offset in AMOUNT_COLUMNS.items()}
        rows.append({
            'YEAR': year,
            'ADMIN1': chapters[chapter],
            'PROGRAM 1': None,
            'ADMIN2': f"{programme} {services[programme]}" if programme in services else programme,
            'FUNCTION1': None,
            'ECON1T': econ['econ1'],
            'ECON2T': text(econ['econ2']),
            'ECON3T': text(econ['econ3']),
            'ECON4T': f"{compte} {label.strip()}",
            **{name: 0 if pd.isna(value) else round(value * 1000) for name, value in amounts.items()},
            'Code': row[0].split(' :')[0].strip(),  # SOURCE_FIN, Source2, road_m, road stay empty as in the sheet
        })
    return pd.DataFrame(rows, columns=COLUMNS)

# COMMAND ----------

for year in years:
    outfile = f'{raw_microdata_csv_dir}/{year}.csv'
    if os.path.exists(outfile):  # skip years already extracted; delete the CSV to force a refresh
        continue
    statement = read_statement(year)
    chapters, services = header_labels(statement)
    df = convert(statement, year, chapters, services)
    assert len(df) > 0, f"{year}: no coded lines found"
    totals = ", ".join(f"{name}={df[name].sum():,.0f}" for name in AMOUNT_COLUMNS)
    print(f"{year}: {len(df)} rows, {(~df['Code'].str.fullmatch(STANDARD_CODE)).sum()} with a non-standard code; {totals}")
    df.to_csv(outfile, index=False, encoding='utf-8')
