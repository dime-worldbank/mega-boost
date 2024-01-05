# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import openpyxl
import pandas as pd

COUNTRY = 'Burkina Faso'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

disaggregated_data_sheets = ['BOOST', 'BOOST_']
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

    # Make modifications to column names such that the column names of interest match in sheets
    # In Burkina Faso in BOOST_ we modify Approved.1, Revised, Paid and GEO_REGION
    if sheet == 'BOOST_':
        column_mapping = {
            'GEO_REGION': 'GEO1',
            'Approved.1': 'APPROVED',
            'Paid': 'PAID',
            'Revised': 'REVISED'
        }
        # Rename columns based on the mapping
        df.rename(columns=column_mapping, inplace=True)
    
    # Since there are common years between the sheets, we retain the sheet name in the sabed CSV
    df['sheet_name'] = sheet

    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
