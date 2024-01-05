# Databricks notebook source
# MAGIC %pip install openpyxl tqdm

# COMMAND ----------

from glob import glob
from pathlib import Path
import pandas as pd
import os
from tqdm import tqdm
import unicodedata

TOP_DIR = "/dbfs/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"

# COMMAND ----------

# Microdata extraction helper functions

def input_excel_filename(country_name):
    country_excel_files = list(glob(f"{INPUT_DIR}/{country_name}*.xlsx"))
    assert len(country_excel_files) == 1, f'expect there to be 1 {country_name} boost data file, found {len(country_excel_files)}'
    return country_excel_files[0]

def prepare_microdata_csv_dir(country_name):
    microdata_dir = f'{WORKSPACE_DIR}/microdata_csv/{country_name}'
    Path(microdata_dir).mkdir(parents=True, exist_ok=True)
    return microdata_dir

def normalize_cell(cell_value):
    if pd.notna(cell_value) and isinstance(cell_value, str):
        return ''.join(c for c in unicodedata.normalize('NFD', cell_value)
                       if unicodedata.category(c) != 'Mn')
    else:
        return cell_value

def is_named_column(column_name):
    return column_name is not None and "Unnamed" not in str(column_name) and column_name != ''

# COMMAND ----------

def get_cci_metadata(prune=False):
    files = glob(f"{INPUT_DIR}/*.xlsx")
    files.sort(key=os.path.getmtime, reverse=True)

    columns = ["country", "fiscal_year", "data_source", "updated_at"]
    meta_df = pd.DataFrame(columns=columns)

    countries = spark.table('indicator.country').toPandas()
    
    for filename in tqdm(files):
        try:
            for sheet_name in ['Approved', 'Executed']: # quick & dirty way to make sure both sheets exit
                df = pd.read_excel(filename, sheet_name=sheet_name, na_values=['..'])
        except ValueError as e:
            print(f"Error reading {sheet_name} from {filename}")
            print(e)
            continue
        
        meta_columns = {'Country': 'country', 'Fiscal year': 'fiscal_year'}
        country_info = df.loc[:, meta_columns.keys()]\
                         .dropna().drop_duplicates()\
                         .rename(columns=meta_columns)\
                         .iloc[0:1, :]
        assert country_info.shape == (1, 2), f'Unexpected country information {country_info} with shape {country_info.shape} from {filename}'
        
        country_info['data_source'] = filename
        country_info['updated_at'] = os.path.getmtime(filename)
        country_row = country_info.iloc[0]

        # convert country code to country name
        if len(country_row.country) == 3 and country_row.country.upper() == country_row.country:
            country_found = countries[countries.country_code == country_row.country]
            if country_found.empty:
                print(f"Unable to look up country code {country_row.country} from {filename}. Skipping")
                continue
            country_name = country_found.iloc[0].country_name
            country_info['country'] = country_name
        else:
            country_name = country_row.country

        country_in_meta_df = meta_df[meta_df.country == country_name]
        if not country_in_meta_df.empty:
            existing_country = country_in_meta_df.iloc[0]
            file_basename = os.path.basename(filename)
            existing_file_basename = os.path.basename(existing_country.data_source)
            print(f'Skipping {file_basename} because a more recent version of {country_name} already processed: {existing_file_basename} last modified {existing_country.updated_at}')
            if prune:
                dbutils.fs.rm(filename.replace("/dbfs", ""))
                print(f"Pruned {file_basename}")
            continue

        meta_df = pd.concat([meta_df, country_info], ignore_index=True)
        
    return meta_df
