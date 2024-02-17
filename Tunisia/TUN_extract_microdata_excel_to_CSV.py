# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %fs ls /dbfs/mnt/DAP

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("/dbfs/mnt/DAP/data")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd
import re

COUNTRY = 'Tunisia'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd
import re

COUNTRY = 'Tunisia'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

disaggregated_data_sheets = ['BOOST']
for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [re.sub(r'\([^)]*\)', '', col).strip() for col in header]
        
    # Normalize cells
    df = df.applymap(normalize_cell)

    # Remove rows where all values are null
    df = df.dropna(how='all')

    # change some of the names in the subnational regions -- in 2020 the names are in arabic
    code_2_name = {
        '00': 'Projects non distribuees',
        '83': 'Tataouine',
        '82': 'Medenine',
        '81': 'Gabes',
        '73': 'Kebili',
        '72': 'Tozeur',
        '71': 'Gafsa',
        '61': 'Sfax',
        '53': 'Mahdia',
        '52': 'Monastir',
        '51': 'Sousse',
        '43': 'Sidi',
        '42': 'Kasserine',
        '41': 'Kairouan',
        '34': 'Siliana',
        '33': 'El',
        '32': 'Jendouba',
        '31': 'Baja',
        '23': 'Bizerte',
        '22': 'Zaghouan',
        '21': 'Nabeul',
        '14': 'Manouba',
        '13': 'Ben',
        '12': 'Ariana',
        '11': 'Tunis',
        '99': "Interet a l'exterieur",
        'لا ينطبق': None,
        '98': ''
        }
    df['GEO1'] = df.GEO1.map(lambda x: code_2_name.get(str(x)[:2]))

    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
