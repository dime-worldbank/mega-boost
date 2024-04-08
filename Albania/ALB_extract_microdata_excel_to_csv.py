# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import pandas as pd
import numpy as np

SHEET_NAME = 'Data_Expenditures'
MIN_NUM_OF_ROWS = 829223
START_YEAR = 2010
END_YEAR = 2022
COUNTRY = 'Albania'

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
csv_file_path = f'{microdata_csv_dir}/{SHEET_NAME}.csv'

df = pd.read_excel(filename, sheet_name=SHEET_NAME)

# Handle null and unnamed columns
header = [col_name for col_name in df.columns if normalize_cell(is_named_column(col_name))]
df = df[header]
df = df.dropna(how='all')
df = df.applymap(normalize_cell)
df.to_csv(csv_file_path, index=False, encoding='utf-8')

existing_years = df['year'].unique()
expected_years = np.arange(START_YEAR, END_YEAR + 1)


assert len(df) >= MIN_NUM_OF_ROWS, f"Number of rows in sheet are less than expected {MIN_NUM_OF_ROWS}, got {df.shape[0]}."
def check_missing_years(expected_years, existing_years):
    missing_year_values = []
    for value in expected_years:
        if value not in existing_years:
            missing_year_values.append(value)

    assert not missing_year_values, f"The following years are missing from the set: {missing_year_values}"

check_missing_years(expected_years, existing_years)
print("All expected years are present")



