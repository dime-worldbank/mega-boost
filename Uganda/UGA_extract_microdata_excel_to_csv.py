# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Uganda BOOST microdata extraction.
#
# Principle (same as Zimbabwe/Kenya): dump the raw `Expenditure` sheet to CSV with no
# transformation. All classification logic (econ/func tagging), the order-proof
# mutually-exclusive predicates, and the year-aware criteria live in
# UGA_transform_load_dlt.py — never here.
#
# The Expenditure sheet is the microdata that the workbook's `Executed` sheet aggregates
# via SUMIFS. We preserve EVERY named column, including the per-line helper-flag columns
# (`add`, `Transfers`, `social protection`, `pension`, `rail`, `air`, `water`, `security`,
# `health`, `education`, `Tertiary`, `wss`, `assistance`) because the SUMIFS — and therefore
# the transform — reference them as building blocks.

import pandas as pd

COUNTRY = 'Uganda'
RAW_SHEET = 'Expenditure'

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

csv_file_path = f'{microdata_csv_dir}/{RAW_SHEET}.csv'

# data_only is irrelevant for pandas (it reads cached values). The helper-flag columns hold
# the cached "y"/"1" results of the in-sheet IF(SEARCH(...)) formulas; we keep them verbatim.
df = pd.read_excel(filename, sheet_name=RAW_SHEET, header=0)

# Minimal hygiene only:
#  - drop unnamed / blank-header columns (e.g. the trailing empty column AJ)
#  - drop fully blank rows
#  - strip diacritics so downstream string matching is stable
header = [c for c in df.columns if is_named_column(c)]
df = df[header].dropna(how='all')
df = df.applymap(normalize_cell)

df.to_csv(csv_file_path, index=False, encoding='utf-8')
print(f'wrote {len(df):,} rows x {len(df.columns)} cols -> {csv_file_path}')
