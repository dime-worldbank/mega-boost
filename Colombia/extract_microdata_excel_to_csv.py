# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %pip install unidecode

# COMMAND ----------

import re
import pandas as pd
from unidecode import unidecode

COUNTRY = 'Colombia'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

# COMMAND ----------

central = pd.read_excel(filename, sheet_name='Raw data', usecols='A:J')
subnational = pd.read_excel(filename, sheet_name='subnational', usecols='A:F')

# COMMAND ----------

def remove_accent_and_newline(cell):
    return re.sub(r'\s', '', unidecode(cell))

central.columns = central.columns.map(remove_accent_and_newline)
subnational.columns = subnational.columns.map(remove_accent_and_newline)
subnational = subnational.dropna(subset=['Ano']).astype({'Ano': int})

# COMMAND ----------

central

# COMMAND ----------

subnational

# COMMAND ----------

expected_central_cols = ['year', 'func1', 'admin1', 'econ1', 'econ2', 'econ3', 'ApropiacionDefinitiva', 'Pago']
for col in expected_central_cols:
    assert col in central.columns, f'Expect to find {col} in {expected_central_cols}, but did not'

min_num_rows_central = 13962
assert central.shape[0] >= min_num_rows_central, f'Expect to find at least {min_num_rows_central}, but found {central.shape[0]}'

# COMMAND ----------

expected_subnat_cols = ['Ano', 'admin0', 'admin1', 'econ', 'Approved', 'Executed']
for col in expected_subnat_cols:
    assert col in subnational.columns, f'Expect to find {col} in {expected_subnat_cols}, but did not'

min_num_rows_subnat = 27204
assert subnational.shape[0] >= min_num_rows_subnat, f'Expect to find at least {min_num_rows_subnat}, but found {subnational.shape[0]}'

# COMMAND ----------

central.to_csv(f'{microdata_csv_dir}/central.csv', index=False)
subnational.to_csv(f'{microdata_csv_dir}/subnational.csv', index=False)
