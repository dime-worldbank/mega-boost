# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import pandas as pd
import openpyxl
import re

COUNTRY = 'Bangladesh'
SPECIAL_INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries Special"
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)

filename = f"{SPECIAL_INPUT_DIR}/{COUNTRY} (total calculations).xlsx"
workbook = openpyxl.open(filename, read_only=True, data_only=True)

for sheet_name in workbook.sheetnames:
    if not sheet_name.startswith('2'):
        continue
    
    sheet = workbook[sheet_name]
    csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'

    if dbfs_file_exists(csv_file_path.replace('/dbfs', '')):
        print(f'{csv_file_path} already exists, skipping extraction')
        continue

    print(f'Reading {COUNTRY} BOOST coded microdata from sheet {sheet_name}')
    df = pd.read_excel(filename, sheet_name=sheet_name, header=0)

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [re.sub(r'\W+', '_', str(col).strip()) for col in header]
    
    df = df.applymap(normalize_cell)

    df.to_csv(csv_file_path, index=False, encoding='utf-8')

workbook.close()
