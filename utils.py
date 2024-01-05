# Databricks notebook source
# MAGIC %pip install openpyxl tqdm

# COMMAND ----------

from glob import glob
from pathlib import Path
import pandas as pd
import os
from tqdm import tqdm
import unicodedata

TOP_DIR = "/dbfs/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"

# COMMAND ----------

# Microdata extraction helper functions

def input_excel_filename(country_name):
    country_excel_files = list(glob(f"{INPUT_DIR}/{country_name}*.xlsx"))
    assert len(country_excel_files) == 1, f'expect there to be 1 {country_name} boost data file, found {len(country_excel_files)}'
    return country_excel_files[0]

def prepare_microdata_csv_dir(country_name):
    microdata_dir = f'{WORKSPACE_DIR}/microdata_csv/{country_name}'
    Path(microdata_dir).mkdir(parents=True, exist_ok=True)
    return microdata_dir

def normalize_cell(cell_value):
    if pd.notna(cell_value) and isinstance(cell_value, str):
        return ''.join(c for c in unicodedata.normalize('NFD', cell_value)
                       if unicodedata.category(c) != 'Mn')
    else:
        return cell_value

def is_named_column(column_name):
    return column_name is not None and "Unnamed" not in str(column_name) and column_name != ''
