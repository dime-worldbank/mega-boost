# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd

def correct_adm1_name(name):
    name_change = {
        'PEMA GATSHEL': 'Pemagatshel',
        'TASHI YANGTSE': 'Trashiyangtse',
        'TASHIGANG': 'Trashigang',
        'THIMPHU':'Thimphu',
        'WANGDUE PHODANG':'Wangduephodrang'
        }
    if type(name)!=str:
        return
    for k,v in name_change.items():
        if k in name:
            name = name.replace(k,v)
    return name.replace('DZONGKHAG', '').strip().title()
    
COUNTRY = 'Bhutan'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

disaggregated_data_sheets = ['BOOST']
for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [col.strip() for col in header]
        
    # Normalize cells
    df = df.applymap(normalize_cell)

    # Remove rows where all values are null
    df = df.dropna(how='all')

    # match names to the region names in subnational population
    df['Admin2'] = df.Admin2.map(lambda x: correct_adm1_name(x) if str(x)[0]=='4' else x)    
    
    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
