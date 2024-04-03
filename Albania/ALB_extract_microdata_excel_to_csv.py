# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import pandas as pd
MIN_NUM_OF_ROWS = 829223
YEAR_RANGE = (2010, 2022)

COUNTRY = 'Albania'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
sheet_name = 'Data_Expenditures'
csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'

df = pd.read_excel(filename, sheet_name=sheet_name)

# Handle null and unnamed columns
header = [col_name for col_name in df.columns if normalize_cell(is_named_column(col_name))]
df = df[header]
df = df.dropna(how='all')
df = df.applymap(normalize_cell)
df.to_csv(csv_file_path, index=False, encoding='utf-8')

assert len(df) >= MIN_NUM_OF_ROWS, f"Number of rows in sheet are less than expected {MIN_NUM_OF_ROWS}, got {df.shape[0]}."
assert df['year'].between(*YEAR_RANGE).all(), f"Values in 'Year' column in sheet are not within the range {YEAR_RANGE}."
