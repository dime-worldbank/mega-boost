# Databricks notebook source
# MAGIC %pip install openpyxl tqdm

# COMMAND ----------

from tqdm import tqdm
from glob import glob
from pathlib import Path
import unicodedata
import pandas as pd
import openpyxl

TOP_DIR = "/dbfs/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Pakistan'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

pak_excel_files = list(glob(f"{INPUT_DIR}/{COUNTRY}*.xlsx"))
assert len(pak_excel_files) == 1, f'expect there to be 1 Pakistan boost data file, found {len(pak_excel_files)}'
filename = pak_excel_files[0]

Path(COUNTRY_MICRODATA_DIR).mkdir(parents=True, exist_ok=True)

# Helper functions
def normalize_cell(cell_value):
    if pd.notna(cell_value) and isinstance(cell_value, str):
        return ''.join(c for c in unicodedata.normalize('NFD', cell_value)
                       if unicodedata.category(c) != 'Mn')
    else:
        return cell_value

def is_named_column(column_name):
    return column_name is not None and "Unnamed" not in str(column_name) and column_name != ''


disaggregated_data_sheets = ['Raw']
for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{COUNTRY_MICRODATA_DIR}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
        
    # Normalize cells
    df = df.applymap(normalize_cell)

    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

# COMMAND ----------


