# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Zimbabwe BOOST microdata extraction.
#
# Principle: dump raw sheets to CSV with no transformation. Any cleaning,
# filtering, or formula emulation belongs in ZWE_transform_load_raw_dlt.py.
# If a value looks wrong, REPORT it (Zimbabwe/_analysis/ISSUES.md) — do not fix.

import pandas as pd

COUNTRY = 'Zimbabwe'
# Raw microdata sheets — Approved/Executed are formula sheets and are NOT dumped.
# They are the reconciliation target, processed by the quality pipeline.
RAW_SHEETS = ['Expenditure', 'Revenue']

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

for sheet_name in RAW_SHEETS:
    csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'
    print(f'Reading {sheet_name}…')
    # data_only equivalent: read_excel returns cached values, not formulas — fine for raw sheets.
    df = pd.read_excel(filename, sheet_name=sheet_name)

    # Minimal hygiene only: drop fully-blank rows and Unnamed columns.
    # No type coercion, no value rewrites — preserves Excel-as-source-of-truth.
    header = [c for c in df.columns if is_named_column(c)]
    df = df[header].dropna(how='all')

    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    print(f'  wrote {len(df):,} rows → {csv_file_path}')
