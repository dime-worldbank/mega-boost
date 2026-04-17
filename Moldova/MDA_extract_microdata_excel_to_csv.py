# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Moldova BOOST microdata extraction.
#
# Principle: dump raw sheets to CSV with no transformation. Any cleaning,
# filtering, or formula emulation belongs in MDA_transform_load_raw_dlt.py.
# If a value looks wrong, REPORT it (Moldova/_onboarding/reports/ISSUES.md) —
# do not fix.
#
# Moldova's raw data is split across three year-range sheets, each with its
# own column schema:
#   - 2006-15 : 11 cols (year, admin1, func1/2, econ1/2, exp_type, transfer,
#               approved, adjusted, executed)
#   - 2016-19 : 22 cols (adds admin2, func3, econ0-6, fin_source1, revised,
#               program1/2, activity)
#   - 2020-24 : 20 cols (as 2016-19 minus program/activity, plus adjusted)
# The hidden `Raw2` sheet is referenced by 40 supplemental formulas (see
# tag_rules.csv rows where n_sumifs > 1); its different layout
# (admin6 instead of admin1, no econ0) prevents a clean merge, so it's
# skipped here and those additive terms are flagged in ISSUES.md for SME
# triage. `LEGEND` is SME notes, not data.
#
# Runs both on Databricks (via the `%run ../utils` magic above, which
# injects `prepare_microdata_csv_dir` / `input_excel_filename`) and
# locally — see `Moldova/README.md` for local-mode instructions.

import os
from pathlib import Path

import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

COUNTRY = 'Moldova'
RAW_SHEETS = ['2006-15', '2016-19', '2020-24']


def _is_named_column(c):
    """Strip pandas' synthetic `Unnamed: N` columns from trailing blank cells."""
    return c is not None and "Unnamed" not in str(c) and str(c).strip() != ''


if IS_DATABRICKS:
    # On Databricks the `%run ../utils` magic exposes the shared helpers.
    microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)  # noqa: F821
    filename = input_excel_filename(COUNTRY)  # noqa: F821
else:
    filename = (os.environ.get('INPUT_FILE_NAME')
                or input("Enter the path to Moldova BOOST.xlsx: ").strip())
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
