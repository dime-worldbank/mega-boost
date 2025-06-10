# Databricks notebook source
# MAGIC %pip install openpyxl tqdm xlsxwriter

# COMMAND ----------

from glob import glob
from pathlib import Path
import pandas as pd
import os
from tqdm import tqdm
import unicodedata

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
RAW_INPUT_DIR = f"{TOP_DIR}/Documents/input/Data from authorities"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"

TAG_MAPPING_URL = 'https://raw.githubusercontent.com/dime-worldbank/mega-boost/refs/heads/main/quality/tag_code_mapping.csv'

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

def prepare_raw_microdata_csv_dir(country_name):
    microdata_dir = f'{WORKSPACE_DIR}/raw_microdata_csv/{country_name}'
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

# Check if the given file path already exists on DBFS
def dbfs_file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise
