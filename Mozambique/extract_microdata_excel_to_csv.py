# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import re
import unicodedata
from pathlib import Path
import openpyxl
import csv
import pandas as pd
import sys
import os

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

COUNTRY = 'Mozambique'

# If there are multiple data sheets, we use the same header to ensure consistent column names
cleaned_header = None

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)

filename = input_excel_filename(COUNTRY)
workbook = openpyxl.open(filename, read_only=True, data_only=True)

for sheet_name in workbook.sheetnames:
    if not sheet_name.startswith('2'):
        continue
    print(f'Reading {COUNTRY} BOOST coded microdata from sheet {sheet_name}')
    sheet = workbook[sheet_name]
    csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'

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

# COMMAND ----------

# adm5 from master key file is needed to map admin 1 names to WB admin1 labels
MASTER_KEY_FILE = f"{TOP_DIR}/Documents/input/Auxiliary/MozambiqueMasterKeys.xlsx"
AUXILIARY_CSV_DIR = f'{WORKSPACE_DIR}/auxiliary_csv/Mozambique'

Path(AUXILIARY_CSV_DIR).mkdir(parents=True, exist_ok=True)

adm5 = pd.read_excel(MASTER_KEY_FILE, sheet_name='Admin Master Key', usecols='N:Q')\
         .dropna(how='any')
adm5 = adm5.rename(columns={adm5.columns[0]: 'UGB_third'})
adm5.columns = adm5.columns.str.replace('\W', '', regex=True)
adm5.to_csv(f'{AUXILIARY_CSV_DIR}/adm5.csv', index=False)
adm5
