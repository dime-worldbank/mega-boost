# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd

COUNTRY = 'Albania'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
disaggregated_data_sheets = ['Data_Expenditures', 'Data_Revenues']

for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)
    # Handle null and unnamed columns
    header = [col_name for col_name in df.columns if normalize_cell(is_named_column(col_name))]
    df = df[header]
    
    # Remove rows where all values are null
    df = df.dropna(how='all')

    # Normalize cells
    df = df.applymap(normalize_cell)

    # Write to csv
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
