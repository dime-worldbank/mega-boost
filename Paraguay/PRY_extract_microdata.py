# Databricks notebook source
# MAGIC %pip install openpyxl tqdm

# COMMAND ----------

from glob import glob
from tqdm import tqdm
import re
import unicodedata
import pandas as pd
from pathlib import Path
import openpyxl
import csv
import pandas as pd

TOP_DIR = "/dbfs/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Paraguay'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

pry_excel_files = list(glob(f"{INPUT_DIR}/{COUNTRY}*.xlsx"))
assert len(pry_excel_files) == 1, f'expect there to be 1 Paraguay boost data file, found {len(pry_excel_files)}'
filename = pry_excel_files[0]

Path(COUNTRY_MICRODATA_DIR).mkdir(parents=True, exist_ok=True)

# Helper functions
def normalize_cell(cell):
    return unicodedata.normalize('NFKD', str(cell))

def is_named_column(cell):
    return cell.value is not None and "Unnamed" not in str(cell.value) and cell.value != ''

# TODO: Figure out the difference between cen and Municipalidades
disaggregated_data_sheets = ['cen']

# COMMAND ----------


for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{COUNTRY_MICRODATA_DIR}/{sheet}.csv'
    # Open the Excel file with openpyxl
    wb = openpyxl.load_workbook(filename, read_only=True)
    sheet_data = wb[sheet]

    # Write the data to a CSV file
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header row
        full_header_row = [cell.value for cell in next(sheet_data.iter_rows(max_row=1))]
        mask = [is_named_column(cell) for cell in next(sheet_data.iter_rows(max_row=1))]
        header_row = [x for x, include in zip(full_header_row, mask) if include]
        csv_writer.writerow(header_row)
        
        # Write data rows
        for row in sheet_data.iter_rows(min_row=2):
            normalized_row = [normalize_cell(cell.value) for i, cell in enumerate(row) if mask[i]]
            csv_writer.writerow(normalized_row)


# COMMAND ----------


