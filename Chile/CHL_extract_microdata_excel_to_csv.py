# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import pandas as pd
import numpy as np

COUNTRY = 'Chile'

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
disaggregated_data_sheets = ['Cen', 'Mun', 'Mun2']

for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)

    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [col.strip() for col in header]
        
    df = df.applymap(normalize_cell)
    df = df.dropna(how='all')
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
