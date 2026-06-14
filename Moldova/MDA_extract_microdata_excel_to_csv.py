# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Moldova BOOST microdata extraction.
#
# Principle (same as Uganda/Zimbabwe/Kenya): dump the raw microdata sheet(s) to CSV with no
# transformation. All classification logic (econ/func tagging), the order-proof mutually-exclusive
# predicates, and the year-aware criteria live in MDA_transform_load_dlt.py — never here.
#
# Moldova differs from single-sheet countries in two ways that this script must respect:
#   1. The microdata is split across THREE era sheets, each its own column layout and (crucially)
#      its own criteria LANGUAGE/coding, which the workbook's `Executed` SUMIFS reach via three
#      parallel sets of defined names:
#         '2006-15'  -> unsuffixed named ranges (year, func1, econ2, exp_type, executed, ...)  English
#         '2016-19'  -> the *_16 named ranges                                                  Romanian
#         '2020-24'  -> the *_20 named ranges                                                  Romanian
#      We write one CSV per era sheet. The transform unions them (bronze) and branches by year.
#      Shared base column names (year, func1, func2, econ1, econ2, exp_type, transfer, executed,
#      approved) line up across eras; era-only columns (admin2, func3, econ0, econ3..6, program*,
#      activity, revised/adjusted) are simply absent in the other eras' CSVs (unionByName fills NULL).
#   2. The sheets are large (~2.3M rows total; the 2016-19 sheet is ~0.9 GB uncompressed XML), so we
#      STREAM with openpyxl read_only and write rows incrementally rather than pd.read_excel — same
#      raw-dump result, bounded memory.
#
# NOTE: a few water-&-sanitation / energy-power *leaf* codes in the 2006-15 era also compose extra
# project rows from a separate `Raw2` sheet (referenced by direct cell range, not a named range).
# Those are func_sub leaves, out of scope for the top-level econ/func onboarding; `Raw2` is therefore
# not extracted here. See verification.md.

import csv
import os

import openpyxl

# The three era microdata sheets the Executed SUMIFS aggregate (sheet name -> output CSV stem).
ERA_SHEETS = ["2006-15", "2016-19", "2020-24"]


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
    for sheet in ERA_SHEETS:
        stream_sheet_to_csv(xlsx_path, sheet, os.path.join(out_dir, f"{sheet}.csv"))


# COMMAND ----------

# Databricks entry point (paths from utils). Mirrors Uganda's extract, looped over the era sheets.
try:
    COUNTRY = "Moldova"
    microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)  # noqa: F821 (from %run ../utils)
    filename = input_excel_filename(COUNTRY)                # noqa: F821
    extract_all(filename, microdata_csv_dir)
except NameError:
    # Not on Databricks (utils helpers absent) -> allow local CLI use:
    #   python MDA_extract_microdata_excel_to_csv.py --workbook "../temp/Moldova BOOST.xlsx" --out /tmp/mda_microdata
    if __name__ == "__main__":
        import argparse
        here = os.path.dirname(os.path.abspath(__file__))
        ap = argparse.ArgumentParser()
        ap.add_argument("--workbook", default=os.path.join(here, "..", "temp", "Moldova BOOST.xlsx"))
        ap.add_argument("--out", default="/tmp/mda_microdata")
        a = ap.parse_args()
        extract_all(a.workbook, a.out)
