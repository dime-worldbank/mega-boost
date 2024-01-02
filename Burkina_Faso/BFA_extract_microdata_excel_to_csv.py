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
COUNTRY = 'Burkina Faso'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

bfa_excel_files = list(glob(f"{INPUT_DIR}/{COUNTRY}*.xlsx"))
assert len(bfa_excel_files) == 1, f'expect there to be 1 Burkina Faso boost data file, found {len(bfa_excel_files)}'
filename = bfa_excel_files[0]

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

disaggregated_data_sheets = ['BOOST', 'BOOST_']

for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{COUNTRY_MICRODATA_DIR}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [col.strip() for col in header]
        
    # Normalize cells
    df = df.applymap(normalize_cell)

    # Remove rows where all values are null
    df = df.dropna(how='all')

    # Make modifications to column names such that the column names of interest match in sheets
    # In Burkina Faso in BOOST_ we modify Approved.1, Revised, Paid and GEO_REGION
    if sheet == 'BOOST_':
        column_mapping = {
            'GEO_REGION': 'GEO1',
            'Approved.1': 'APPROVED',
            'Paid': 'PAID',
            'Revised': 'REVISED'
        }
        # Rename columns based on the mapping
        df.rename(columns=column_mapping, inplace=True)
    
    # Since there are common years between the sheets, we retain the sheet name in the sabed CSV
    df['sheet_name'] = sheet

    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')


# COMMAND ----------


