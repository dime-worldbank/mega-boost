# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import os
import re
from glob import glob
from pathlib import Path
import openpyxl
import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
COUNTRY = 'Bangladesh'
if IS_DATABRICKS:
    # RAW_INPUT_DIR, TOP_DIR and prepare_raw_microdata_csv_dir come from %run ../utils
    KEYS_FILE = f"{TOP_DIR}/Documents/input/Auxiliary/Bangladesh Keys 23-25.xlsx"
    raw_microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)
else:
    RAW_INPUT_DIR = os.environ.get('RAW_INPUT_DIR') or input("Enter the raw input directory (containing Bangladesh/other/): ").strip()
    KEYS_FILE = os.environ.get('KEYS_FILE') or input("Enter the Keys workbook path: ").strip()
    raw_microdata_csv_dir = os.environ.get('OUTPUT_DIR') or input("Enter the output directory: ").strip()
    Path(raw_microdata_csv_dir).mkdir(parents=True, exist_ok=True)

EXPORT_COLUMNS = {
    'ADMIN1': 'admin1', 'ADMIN2': 'admin2', 'ADMIN3': 'admin3', 'ADMIN4': 'admin4',
    'GEO1': 'geo1', 'GEO2': 'geo2', 'GEO3': 'geo3', 'BUDGETTYPE': 'budget_type', 'FUND': 'fund',
    'ECON6': 'econ6', 'EXECUTED': 'executed',
}
OUTPUT_COLUMNS = [
    'year', 'admin1', 'admin2', 'admin3', 'admin4', 'geo1', 'geo2', 'geo3', 'budget_type', 'fund',
    'econ1', 'econ2', 'econ3', 'econ4', 'econ5', 'econ6',
    'admin2_standardized', 'func1', 'func_group', 'executed',
]

# COMMAND ----------

def leading_code(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    match = re.match(r'\s*(\d+)', str(value))
    return match.group(1) if match else None


def _clean(value):
    return value.strip() if isinstance(value, str) else value


def read_keys(path):
    """Massimo's lookups: ECON (7-digit econ code -> ECON 1..5 labels) and BT-ADM
    (3-digit ministry code -> ADMIN2_STANDARDIZED, FUNC1, FUNC_GROUP)."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        econ_by_code7 = {}
        for row in wb['ECON'].iter_rows(min_row=2, values_only=True):
            code7 = leading_code(row[0])
            if code7:
                econ_by_code7.setdefault(code7, tuple(_clean(v) for v in row[1:6]))
        admin_by_code3 = {}
        for row in wb['BT-ADM'].iter_rows(min_row=3, values_only=True):
            code3 = leading_code(row[0])
            if code3:
                admin_by_code3.setdefault(code3, tuple(_clean(v) for v in row[2:5]))
    finally:
        wb.close()
    return econ_by_code7, admin_by_code3


def export_year(path):
    """End year of the export's title row, e.g. 'FISCAL YEAR : 2021-22' -> 2022."""
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        title = next(wb.worksheets[0].iter_rows(max_row=1, values_only=True))[0]
    finally:
        wb.close()
    match = re.search(r'(\d{2})(\d{2})\s*-\s*(\d{2})', str(title or ''))
    if not match:
        raise ValueError(f"{os.path.basename(path)}: no fiscal year in title row {title!r}")
    return int(match.group(1) + match.group(3))


def read_export(path):
    year = export_year(path)
    df = pd.read_excel(path, header=2, dtype=str)
    df['EXECUTED'] = pd.to_numeric(df['EXECUTED'])
    df = df[df['EXECUTED'].notna()]
    df = df[list(EXPORT_COLUMNS)].rename(columns=EXPORT_COLUMNS)
    df = df.apply(lambda s: s.map(_clean))
    df.insert(0, 'year', year)
    return year, df


def is_stale(csv_path, *input_paths):
    if not os.path.exists(csv_path):
        return True
    return os.path.getmtime(csv_path) < max(os.path.getmtime(p) for p in input_paths)


def _unmapped(df, mask, column):
    totals = df[mask].groupby(column, dropna=False)['executed'].sum()
    return [f"  {value!r}: {total:,.0f}" for value, total in totals.items()]


def classify(df, econ_by_code7, admin_by_code3):
    # The 5-digit prefix is the parent ECON 5, so any 7-digit child under it carries the same
    # ECON 1..5 labels; exports add new 7-digit codes before the Keys catch up.
    econ_by_code5 = {}
    for code7, labels in econ_by_code7.items():
        econ_by_code5.setdefault(code7[:5], labels)

    code7 = df['econ6'].map(leading_code)
    econ = code7.map(econ_by_code7)
    fallback = econ.isna() & code7.notna()
    econ[fallback] = code7[fallback].str[:5].map(econ_by_code5)
    used_fallback = fallback & econ.notna()
    if used_fallback.any():
        print(f"{df['year'].iloc[0]}: {used_fallback.sum()} rows "
              f"({df.loc[used_fallback, 'executed'].sum():,.0f} executed) mapped by 5-digit econ parent, "
              f"codes {sorted(code7[used_fallback].unique())}")

    admin = df['admin2'].map(lambda v: (leading_code(v) or '')[:3]).map(admin_by_code3)

    problems = []
    if econ.isna().any():
        problems += ["ECON6 not in Keys ECON (by 7 or 5 digits):"] + _unmapped(df, econ.isna(), 'econ6')
    if admin.isna().any():
        problems += ["ADMIN2 not in Keys BT-ADM (by 3-digit code):"] + _unmapped(df, admin.isna(), 'admin2')
    if problems:
        raise ValueError(f"{df['year'].iloc[0]}: unmapped codes\n" + "\n".join(problems))

    df = df.copy()
    for i, column in enumerate(['econ1', 'econ2', 'econ3', 'econ4', 'econ5']):
        df[column] = econ.map(lambda labels: labels[i])
    for i, column in enumerate(['admin2_standardized', 'func1', 'func_group']):
        df[column] = admin.map(lambda values: values[i])
    return df[OUTPUT_COLUMNS]

# COMMAND ----------

export_paths = sorted(glob(f"{RAW_INPUT_DIR}/{COUNTRY}/other/EXP_*.xlsx"))
paths_by_year = {}
for path in export_paths:
    paths_by_year.setdefault(export_year(path), []).append(path)
duplicates = {year: paths for year, paths in paths_by_year.items() if len(paths) > 1}
if duplicates:
    raise ValueError(f"More than one export per year: {duplicates}")

stale = {year: paths[0] for year, paths in paths_by_year.items()
         if is_stale(f"{raw_microdata_csv_dir}/{year}.csv", KEYS_FILE, paths[0])}
for year in sorted(set(paths_by_year) - set(stale)):
    print(f"{year}.csv is newer than its export and the Keys file, skipping")

if stale:
    econ_by_code7, admin_by_code3 = read_keys(KEYS_FILE)
    for year, path in sorted(stale.items()):
        print(f"Extracting {os.path.basename(path)} -> {year}.csv")
        _, df = read_export(path)
        classify(df, econ_by_code7, admin_by_code3).to_csv(
            f"{raw_microdata_csv_dir}/{year}.csv", index=False, encoding='utf-8')
