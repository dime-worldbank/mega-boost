# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

from glob import glob
import re
import unicodedata
from pathlib import Path
import openpyxl
import csv

TOP_DIR = "/dbfs/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Mozambique'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

moz_excel_files = list(glob(f"{INPUT_DIR}/{COUNTRY}*.xlsx"))
assert len(moz_excel_files) == 1, f'expect there to be 1 Mozambique boost data file, found {len(moz_excel_files)}'

Path(COUNTRY_MICRODATA_DIR).mkdir(parents=True, exist_ok=True)

# If there are multiple data sheets, we use the same header to ensure consistent column names
cleaned_header = None

filename = moz_excel_files[0]
workbook = openpyxl.open(filename, read_only=True, data_only=True)

for sheet_name in workbook.sheetnames:
    if not sheet_name.startswith('2'):
        continue
    print(f'Reading {COUNTRY} BOOST coded microdata from sheet {sheet_name}')
    sheet = workbook[sheet_name]
    csv_file_path = f'{COUNTRY_MICRODATA_DIR}/{sheet_name}.csv'

    with open(csv_file_path, 'w', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        rows_iter = sheet.rows
        
        # Always move forward the iterator even if we don't use the header row to write to csv
        header_row = next(rows_iter)
        
        if not cleaned_header:        
            header_cells = []
            for cell in header_row:
                normalized_string = unicodedata.normalize('NFKD', str(cell.value))
                cleaned_string = re.sub(r'\W+', '', normalized_string)
                header_cells.append(cleaned_string)

            # Remove the trailing "None" columns
            header_cells_reversed = list(reversed(header_cells))
            last_none = -1
            for heading in header_cells_reversed:
                if heading == 'None':
                    last_none += 1
                else:
                    break
            cleaned_header = list(reversed(header_cells_reversed[last_none+1:]))
            
        csv_writer.writerow(cleaned_header)

        for row in rows_iter:
            year_cell = row[0].value
            exp_type_cell = row[1].value
            if year_cell is None or year_cell == '' or exp_type_cell is None or exp_type_cell == '':
                continue
            csv_writer.writerow([cell.value for i, cell in enumerate(row) if i < len(cleaned_header)])

workbook.close()
