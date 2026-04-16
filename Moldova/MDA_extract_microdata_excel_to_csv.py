# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Moldova BOOST microdata extraction.
#
# Principle: dump raw sheets to CSV with no transformation. Any cleaning,
# filtering, or formula emulation belongs in MDA_transform_load_raw_dlt.py.
# If a value looks wrong, REPORT it (Moldova/_analysis/reports/ISSUES.md) —
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

import pandas as pd

COUNTRY = 'Moldova'
RAW_SHEETS = ['2006-15', '2016-19', '2020-24']

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

for sheet_name in RAW_SHEETS:
    csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'
    print(f'Reading {sheet_name}…')
    df = pd.read_excel(filename, sheet_name=sheet_name)
    header = [c for c in df.columns if is_named_column(c)]
    df = df[header].dropna(how='all')
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    print(f'  wrote {len(df):,} rows → {csv_file_path}')
