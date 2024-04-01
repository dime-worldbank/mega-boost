# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd

COUNTRY = 'Albania'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
disaggregated_data_sheets = ['Data_Expenditures']

for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet)
    # Handle null and unnamed columns
    header = [col_name for col_name in df.columns if normalize_cell(is_named_column(col_name))]
    df = df[header]
    df = df.dropna(how='all')
    df = df.applymap(normalize_cell)
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

    min_num_of_rows = 829223
    year_range = (2010,2022)

    assert len(df) >= min_num_of_rows, f"Number of rows in sheet are less than 829223."
    assert df['year'].between(*year_range).all(), f"Values in 'Year' column in sheet are not within the range {year_range}."

