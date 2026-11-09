# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Central African Republic BOOST microdata extraction.
#
# Principle: dump raw sheets to CSV with no transformation. Any cleaning,
# filtering, or formula emulation belongs in CAF_transform_load_raw_dlt.py.
# If a value looks wrong, REPORT it (Central_Africa_Republic/_onboarding/
# reports/ISSUES.md) — do not fix.
#
# The workbook `Central African Republic BOOST.xlsx` holds one `Expenditure`
# sheet for every year (2016-2025, 18 columns: YEAR, ADMIN1, PROGRAM 1,
# ADMIN2, FUNCTION1, ECON1T-ECON4T, SOURCE_FIN, Source2, Dotation, Engagé,
# Liquidé, Ordonnancé, Ord. PEC, road_m, road) and one `revenue` sheet
# (YEAR, ECON1-ECON4, approved, Actuals). `Approved` / `Executed` are the
# formula sheets whose SUMIFS the pipeline reproduces — they are not dumped.
# `old 2022` (the pre-SIM_ba 2022 extract) is not referenced by any named
# range and is left out. The whole sheet is dumped, but the pipeline only
# takes years before 2023 from it: 2023 onward comes straight from the DGB
# statement through CAF_extract_raw_microdata_excel_to_csv.py
# (raw_microdata_csv/), the Albania pattern.
#
# Runs both on Databricks (via the `%run ../utils` magic above, which
# injects `prepare_microdata_csv_dir` / `input_excel_filename`) and locally
# with INPUT_FILE_NAME / OUTPUT_DIR in the environment (prompted otherwise).

import os
from pathlib import Path

import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

COUNTRY = 'Central African Republic'
RAW_SHEETS = ['Expenditure', 'revenue']


def _is_named_column(c):
    """Strip pandas' synthetic `Unnamed: N` columns from trailing blank cells."""
    return c is not None and "Unnamed" not in str(c) and str(c).strip() != ''


if IS_DATABRICKS:
    microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)  # noqa: F821
    filename = input_excel_filename(COUNTRY)  # noqa: F821
else:
    filename = (os.environ.get('INPUT_FILE_NAME')
                or input(f"Enter the path to {COUNTRY} BOOST.xlsx: ").strip())
    microdata_csv_dir = (os.environ.get('OUTPUT_DIR')
                         or input("Enter output directory for raw CSVs: ").strip())
    Path(microdata_csv_dir).mkdir(parents=True, exist_ok=True)

for sheet_name in RAW_SHEETS:
    csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'
    print(f'Reading {sheet_name}…')
    df = pd.read_excel(filename, sheet_name=sheet_name)
    header = [c for c in df.columns if _is_named_column(c)]
    df = df[header].dropna(how='all')
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    print(f'  wrote {len(df):,} rows → {csv_file_path}')
