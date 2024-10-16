# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import pandas as pd

COUNTRY = 'South Africa'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

sheet = 'raw'

csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
df = pd.read_excel(filename, sheet_name=sheet, header=0)

# Handle unnamed or null named columns
header = [col_name for col_name in df.columns if is_named_column(col_name)]
df = df[header]
df.columns = [col.strip() for col in header]
    
# Normalize cells
df = df.applymap(normalize_cell)
df = df.dropna(how='all')

df.to_csv(csv_file_path, index=False, encoding='utf-8')

# COMMAND ----------

import pandas as pd
import pandas as pd

COUNTRY = 'South Africa'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

sheet = 'raw'

csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
test = pd.read_csv(csv_file_path)

# COMMAND ----------

programs = list(test['Programme'].unique())

# COMMAND ----------

econ1 = list(test['Economic Level 1'].unique())

# COMMAND ----------

econ2 = list(test['Economic Level 2'].unique())
econ3 = list(test['Economic Level 3'].unique())

# COMMAND ----------

econ2

# COMMAND ----------



# COMMAND ----------

func2 = list(test['Budget group'].unique())
func1 = list(test['Function group'].unique())

# COMMAND ----------

transfers = list(test['Transfers and subsidies / Payments for financial assets detail'].unique())

# COMMAND ----------

# DBTITLE 1,est
test['Admin1'].unique()

# COMMAND ----------

transfers = [str(t).lower() for t in transfers]
