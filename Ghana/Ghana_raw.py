# Databricks notebook source
!pip install xlrd

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

import pandas as pd

# COMMAND ----------

path_MDA_2022 = '/Volumes/prd_mega/sboost4/vboost4/Documents/input/Data from authorities/MDA_WorldBank _Data_2022_with_Budget/CF_MDA_WorldBank_Data_FY2022.xls'

# COMMAND ----------

excel_file = pd.ExcelFile(path_MDA_2022)
sheet_names = excel_file.sheet_names
display(pd.DataFrame(sheet_names, columns=['Sheet Names']))

# COMMAND ----------

mda_2022 = pd.read_excel(path_MDA_2022, sheet_name='Sheet 1')

# COMMAND ----------

# Compute top‑10 most frequent values for each column in mda_2022
top_n = 50
col_top_vals = {}
max_len = top_n

for col in mda_2022.columns:
    # Get the most common values (as a list)
    top_vals = mda_2022[col].value_counts().index.tolist()[:top_n]
    # Pad with None if fewer than top_n distinct values
    top_vals += [None] * (max_len - len(top_vals))
    col_top_vals[col] = top_vals

# Build the result DataFrame: first column 'values' holds the rank (1‑10)
result_df = pd.DataFrame({"values": list(range(1, top_n + 1))})
for col, vals in col_top_vals.items():
    result_df[col] = vals

# Show the table
display(result_df)

# COMMAND ----------

bronze = spark.table('prd_mega.boost.gha_boost_bronze').toPandas()

# COMMAND ----------

# Compute top‑10 most frequent values for each column in mda_2022
top_n = 50
col_top_vals = {}
max_len = top_n

for col in bronze.columns:
    # Get the most common values (as a list)
    top_vals = bronze[col].value_counts().index.tolist()[:top_n]
    # Pad with None if fewer than top_n distinct values
    top_vals += [None] * (max_len - len(top_vals))
    col_top_vals[col] = top_vals

# Build the result DataFrame: first column 'values' holds the rank (1‑10)
result_df_bronze = pd.DataFrame({"values": list(range(1, top_n + 1))})
for col, vals in col_top_vals.items():
    result_df_bronze[col] = vals

# Show the table
display(result_df_bronze)

# COMMAND ----------


