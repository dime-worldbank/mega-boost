# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Yemen BOOST microdata extraction: the workbook's `BOOST` tab, as a test input for the pipeline.
#
# Dumps the tab to CSV with no transformation. The tab is the microdata that the `Executed` sheet
# aggregates via SUMIFS over its named column ranges (Year, Func, Admin1, Econ1, econ2, Executed), so
# YEM_transform_load_dlt.py run on this extract must reproduce every Executed cell exactly (it does:
# 75 / 75 cells for 2023-2025 -- the total, 7 econ, 10 func, 2 econ_sub and 5 func_sub rows). The
# production input is the rebuild of the yearly sector files (YEM_extract_raw_microdata_excel_to_csv.py,
# same six columns), because the tab's 2025 block is mis-stacked.
#
# One file per year, YYYY.csv, in the same layout as the rebuild, so that the transform runs on this
# folder unchanged: point its COUNTRY_MICRODATA_DIR at microdata_csv/Yemen instead of
# raw_microdata_csv/Yemen.
#
# Minimal hygiene only: the six named columns, blank rows dropped (the tab's used range runs 63,000
# rows past the data and holds one blank row inside it), Year as an integer. Labels are kept verbatim,
# spaces and all, and the tab's six duplicated zero rows of 2025 are kept: the SUMIFS see them too.

import pandas as pd

COUNTRY = 'Yemen'
RAW_SHEET = 'BOOST'
COLUMNS = ['Year', 'Func', 'Admin1', 'Econ1', 'econ2', 'Executed']

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

df = pd.read_excel(filename, sheet_name=RAW_SHEET, header=0, usecols=COLUMNS)
df = df.dropna(how='all')
df['Year'] = df['Year'].astype(int)

for year, part in df.groupby('Year', sort=True):
    csv_file_path = f'{microdata_csv_dir}/{year}.csv'
    part.to_csv(csv_file_path, index=False, float_format='%.17g', lineterminator='\n', encoding='utf-8')
    print(f'wrote {len(part):,} rows -> {csv_file_path}')
