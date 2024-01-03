# Databricks notebook source
# MAGIC %pip install openpyxl tqdm

# COMMAND ----------

from glob import glob
from pathlib import Path
import pandas as pd
import os
from tqdm import tqdm

TOP_DIR = "/dbfs/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"

# COMMAND ----------

def input_excel_filename(country_name):
    country_excel_files = list(glob(f"{INPUT_DIR}/{country_name}*.xlsx"))
    assert len(country_excel_files) == 1, f'expect there to be 1 {country_name} boost data file, found {len(country_excel_files)}'
    return country_excel_files[0]

def prepare_microdata_csv_dir(country_name):
    microdata_dir = f'{WORKSPACE_DIR}/microdata_csv/{country_name}'
    Path(microdata_dir).mkdir(parents=True, exist_ok=True)
    return microdata_dir

# COMMAND ----------

def get_cci_metadata(prune=False):
    files = glob(f"{INPUT_DIR}/*.xlsx")
    files.sort(key=os.path.getmtime, reverse=True)

    columns = ["country", "fiscal_year", "data_source", "updated_at"]
    meta_df = pd.DataFrame(columns=columns)
    
    for filename in tqdm(files):
        try:
            for sheet_name in ['Approved', 'Executed']: # quick & dirty way to make sure both sheets exit
                df = pd.read_excel(filename, sheet_name=sheet_name, na_values=['..'])
        except ValueError as e:
            print(f"Error reading {sheet_name} from {filename}")
            print(e)
            continue
        
        meta_columns = ['Country', 'Fiscal year']
        country_info = df.loc[:, meta_columns]\
                         .dropna().drop_duplicates()\
                         .rename(columns={'Country': 'country', 'Fiscal year': 'fiscal_year'})
        assert country_info.shape == (1, 2), f'Unexpected country information {country_info} from {filename}'
        
        country_info['data_source'] = filename
        country_info['updated_at'] = os.path.getmtime(filename)
        country = country_info.country[0]
        country_in_meta_df = meta_df[meta_df.country == country]
        if not country_in_meta_df.empty:
            existing_country = country_in_meta_df.iloc[0]
            file_basename = os.path.basename(filename)
            existing_file_basename = os.path.basename(existing_country.data_source)
            print(f'Skipping {file_basename} because a more recent version of {country} already processed: {existing_file_basename} last modified {existing_country.updated_at}')
            if prune:
                dbutils.fs.rm(filename.replace("/dbfs", ""))
                print(f"Pruned {file_basename}")
            continue

        meta_df = pd.concat([meta_df, country_info], ignore_index=True)
        
    return meta_df
