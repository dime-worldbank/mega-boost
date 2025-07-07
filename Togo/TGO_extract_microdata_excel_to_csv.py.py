# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import pandas as pd
import openpyxl
import re

COUNTRY = 'Togo'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)

filename = f"{INPUT_DIR}/{COUNTRY} BOOST.xlsx"
sheet_name = "2021_A_2025"
df = pd.read_excel(filename, sheet_name=sheet_name)
df

# COMMAND ----------

df = df[[col for col in df.columns if col and not str(col).startswith("Unnamed")]]
df.columns = [col.strip() for col in df.columns]
df.dropna(how='all', inplace=True)
df

# COMMAND ----------

csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'
df.to_csv(csv_file_path, index=False, encoding='utf-8')
