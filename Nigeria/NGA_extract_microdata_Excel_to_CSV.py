# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd

code_2_adm1 = {'01': 'Abia',
 '02': 'Adamawa',
 '03': 'Akwa Ibom',
 '04': 'Anambra',
 '05': 'Bauchi',
 '06': 'Bayelsa',
 '07': 'Benue',
 '08': 'Borno',
 '09': 'Cross River',
 '10': 'Delta',
 '11': 'Ebonyi',
 '12': 'Edo',
 '13': 'Ekiti',
 '14': 'Enugu',
 '15': 'Gombe',
 '16': 'Imo',
 '17': 'Jigawa',
 '18': 'Kaduna',
 '19': 'Kano',
 '20': 'Katsina',
 '21': 'Kebbi',
 '22': 'Kogi',
 '23': 'Kwara',
 '24': 'Lagos',
 '25': 'Nasarawa',
 '26': 'Niger',
 '27': 'Ogun',
 '28': 'Ondo',
 '29': 'Osun',
 '30': 'Oyo',
 '31': 'Plateau',
 '32': 'Rivers',
 '33': 'Sokoto',
 '34': 'Taraba',
 '35': 'Yobe',
 '36': 'Zamfara',
 '37': 'Federal Capital Territory'}

COUNTRY = 'Nigeria'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)
# There is a sheet for states but it's empty in the source data
disaggregated_data_sheets = ['central']
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

    # Associate the corrected admin1 using the Region column
    df['adm1_name'] = df.Region.map(lambda x: code_2_adm1.get(x[:2]))
    
    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

# COMMAND ----------


