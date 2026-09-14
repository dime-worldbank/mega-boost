# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Bulgaria BOOST microdata extraction.
#
# Principle (same as Uganda/Zimbabwe/Kenya): dump the workbook's raw `Expenditure` sheet to CSV
# with NO transformation. All classification logic (econ/func tagging, the order-proof
# mutually-exclusive predicates) lives in BGR_transform_load_dlt.py -- never here.
#
# The Expenditure sheet is the microdata that the workbook's `Approved`/`Executed` sheets
# aggregate via SUMIFS over the named ranges year / admin1 / func1 / func2 / func3 / econ1 /
# econ2 / source (=fin_source1) / exp_type / transfer / approved (=adjusted) / executed /
# road (=roads) / interest (=Interest). Every named column is preserved, INCLUDING the two
# per-line helper flags `roads` and `Interest` (cached results of the in-sheet lookups
# "road from func3" / "interest from econ1") because the SUMIFS reference them.
#
# The sheet is large (~1.0M rows x 14 named columns; the XML part is ~0.5 GB), so it is
# streamed row by row with openpyxl's read-only mode instead of being loaded whole.
#
# Hygiene applied (nothing else):
#   - drop unnamed / blank-header columns (the sheet has empty styled columns O..V)
#   - drop fully blank rows (~120k rows carry formatting only)
#   - strip diacritics (normalize_cell) so downstream string matching is stable
#   - cell text is NOT trimmed: Excel's SUMIFS matches labels literally, so must the pipeline
#   - the sheet is saved with an active AutoFilter; SUMIFS ignores filters, so ALL rows are kept

import csv

import openpyxl

COUNTRY = 'Bulgaria'
RAW_SHEET = 'Expenditure'


def extract_sheet_to_csv(xlsx_path, sheet_name, csv_path, log_every=200_000):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    rows = ws.iter_rows(values_only=True)
    header = next(rows)
    keep = [i for i, name in enumerate(header) if is_named_column(name)]
    n = 0
    with open(csv_path, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow([str(header[i]) for i in keep])
        for row in rows:
            vals = [row[i] if i < len(row) else None for i in keep]
            if all(v is None for v in vals):
                continue
            w.writerow(['' if v is None else normalize_cell(v) for v in vals])
            n += 1
            if n % log_every == 0:
                print(f'  {n:,} rows')
    wb.close()
    print(f'wrote {n:,} rows x {len(keep)} cols -> {csv_path}')
    return n


# COMMAND ----------

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
csv_file_path = f'{microdata_csv_dir}/{RAW_SHEET}.csv'
extract_sheet_to_csv(filename, RAW_SHEET, csv_file_path)
