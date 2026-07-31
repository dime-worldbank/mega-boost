# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import csv
import os

import openpyxl

# The three era microdata sheets the Executed SUMIFS aggregate (sheet name -> output CSV stem).
MICRO_DATA_SHEETS = ["2006-15", "2016-19", "2020-24"]


def stream_sheet_to_csv(xlsx_path, sheet_name, out_csv):
    """Stream one worksheet to CSV verbatim (row 1 = header), bounded memory."""
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    n = 0
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            # Excel sheets carry many fully-blank trailing rows (the 2006-15 sheet has ~795k); drop
            # them so the CSV is just real budget lines + the header. Keep the header row (i == 0).
            if i and all(c is None or c == "" for c in row):
                continue
            w.writerow(["" if c is None else c for c in row])
            n += 1
    wb.close()
    print(f"  wrote {n:,} rows -> {out_csv}")
    return n


def extract_all(xlsx_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for sheet in MICRO_DATA_SHEETS:
        stream_sheet_to_csv(xlsx_path, sheet, os.path.join(out_dir, f"{sheet}.csv"))


# COMMAND ----------

COUNTRY = "Moldova"
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)  # noqa: F821 (from %run ../utils)
filename = input_excel_filename(COUNTRY)                # noqa: F821
extract_all(filename, microdata_csv_dir)
