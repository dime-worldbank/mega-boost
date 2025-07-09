# Databricks notebook source
import pyspark.pandas as ps
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import re
import numpy as np
database_name = "prd_mega.boost_intermediate"

ps.set_option('compute.ops_on_diff_frames', False)
# Set up Spark session (if running outside Databricks)
spark = SparkSession.builder.getOrCreate()

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/Tunisia'

CSV_READ_OPTIONS = {
    "header": "infer",
    "multiline": True,
    "quotechar": '"',
    "escapechar": '"',
}

def clean_col(col_name):
    return re.sub(r'\s+', '_', col_name.strip().lower())

# Read CSV as pyspark.pandas DataFrame
df = ps.read_csv(COUNTRY_MICRODATA_DIR, **CSV_READ_OPTIONS)
df = df.fillna('')
for col in ['TYPE', 'GBO', 'ADMIN1', 'ADMIN2', 'ECON1', 'ECON2', 'ECON3',
       'ECON4', 'ECON5', 'PROG', 'SPROG', 'GEO1', 'FONDS', 'LOI DE FINANCE',
       'DELEGUE', 'Roads', 'Air', 'WSS','railroads', 'Primary', 'Secondary', 'SOE', 'Maintenance', 'subsidies']:
    df[col] = df[col].astype(str).fillna("")

# Clean column names
df.columns = [clean_col(c) for c in df.columns]
df['year'] = df['year'].astype(int)
df = df[~df['econ2'].astype(str).str.startswith('10')]

# admin0_tmp, admin1_tmp, admin2_tmp
# admin0_tmp: always 'Central'
df['admin0_tmp'] = 'Central'
# admin1_tmp: always 'Central Scope'
df['admin1_tmp'] = 'Central Scope'
# admin2_tmp: remove leading numbers and spaces from admin2
if 'admin2' in df.columns:
    df['admin2_tmp'] = df['admin2'].str.replace(r'^[0-9\s]*', '', regex=True)
else:
    df['admin2_tmp'] = ''

# geo1 logic
if 'geo1' in df.columns:
    df['geo1'] = df['geo1'].fillna('')
    df['geo1'] = df['geo1'].where(~df['geo1'].isnull(), 'Central Scope')
    df['geo1'] = df['geo1'].where(~df['geo1'].str.startswith(('0', '9')), 'Central Scope')
    df['geo1'] = df['geo1'].where(~df['geo1'].str.match(r'^[1-8]'), df['geo1'].str.replace(r'^[1-8]+\s*', '', regex=True))
    df['geo1'] = df['geo1'].replace({'BeBen Arous': 'Ben Arous'})
else:
    df['geo1'] = 'Central Scope'

# is_foreign
if 'econ2' in df.columns:
    df['is_foreign'] = df['econ2'].str.startswith('09')
else:
    df['is_foreign'] = False

# --- Wide columns on the go for func, funcsub, econ, econsub ---
# First, collect all possible categories for each
func_categories = [
    'Housing and community amenities', 'Defence', 'Public order and safety', 'Environmental protection',
    'Health', 'Social protection', 'Education', 'Recreation culture and religion',
    'Economic affairs', 'General public services'
]
funcsub_categories = [
    'Public Safety', 'Judiciary', 'Tertiary Education', 'Agriculture', 'Telecom', 'Transport', "Other expenses"
]
econsub_categories = [
    'Pensions', 'Social Assistance', 'Basic Wages', 'Capital Maintenance',
    'Recurrent Maintenance', 'Subsidies to Production',  'Other expenses'
]
econ_categories = [
    'Wage bill', 'Capital expenditures', 'Goods and services', 'Subsidies',
    'Social benefits', 'Interest on debt', 'Other expenses'
]

# Only create wide columns for func, funcsub, econ, econsub on the go, do not set main columns
import numpy as np

def set_wide_columns(row):
    # FUNC WIDE
    if row.get('wss', '') == '1':
        row['func_housing_and_community_amenities'] = 1
    if row.get('admin1', '').startswith('09') or row.get('admin1', '').startswith('06'):
        row['func_defence'] = 1
    if (row.get('admin1', '').startswith('06') and row.get('admin2', '').startswith('07')) or row.get('admin1', '').startswith('07'):
        row['func_public_order_and_safety'] = 1
    if row.get('admin2', '').startswith('21'):
        row['func_environmental_protection'] = 1
    if row.get('admin2', '').startswith('27') or row.get('admin2', '').startswith('34'):
        row['func_health'] = 1
    if row.get('admin1', '').startswith('05'):
        row['func_social_protection'] = 1
    if row.get('admin2', '')[:2] in ['04', '29', '30', '33', '37', '39', '40']:
        row['func_education'] = 1
    if row.get('admin1', '')[:2] in ['19', '10', '20']:
        row['func_recreation_culture_and_religion'] = 1
    if row.get('admin2', '').startswith('16') or row.get('admin2', '').startswith('17'):
        row['func_economic_affairs'] = 1
    if row.get('admin1', '').startswith('18'):
        row['func_economic_affairs'] = 1
    if row.get('roads', '') == '1' or row.get('railroads', '') == '1' or row.get('air', '') == '1':
        row['func_economic_affairs'] = 1
    # Default: general public services if none above
    if not any([row.get(f'func_{clean_col(cat)}', 0) == 1 for cat in func_categories if cat != 'General public services']):
        row['func_general_public_services'] = 1

    # funcsub WIDE
    if row.get('admin1', '').startswith('06') and row.get('admin2', '').startswith('07'):
        row['funcsub_public_safety'] = 1
    if row.get('admin1', '').startswith('07'):
        row['funcsub_judiciary'] = 1
    if row.get('admin2', '')[:2] in ['04', '30', '33']:
        row['funcsub_tertiary_education'] = 1
    if row.get('admin2', '').startswith('16') or row.get('admin2', '').startswith('17'):
        row['funcsub_agriculture'] = 1
    if row.get('admin1', '').startswith('18'):
        row['funcsub_telecom'] = 1
    if row.get('roads', '') == '1' or row.get('railroads', '') == '1' or row.get('air', '') == '1':
        row['funcsub_transport'] = 1
    if not any([row.get(f'funcsub_{clean_col(cat)}', 0) == 1 for cat in funcsub_categories if cat != "Other expenses"]):
        row['funcsub_other_expenses'] = 1

    # econsub WIDE
    if int(row.get('year', 0)) > 2015 and row.get('prog', '') == '2 Securite Sociale':
        row['econsub_pensions'] = 1
    if row.get('admin1', '').startswith('05') and row.get('prog', '') != '2 Securite Sociale':
        row['econsub_social_assistance'] = 1
    if row.get('econ2', '').startswith('01') and row.get('prog', '') != '2 Securite Sociale':
        row['econsub_basic_wages'] = 1
    if row.get('maintenance', '') == '1' and row.get('econ1', '').startswith('Titre 2'):
        row['econsub_capital_maintenance'] = 1
    if row.get('maintenance', '') == '1' and row.get('econ1', '').startswith('Titre 1'):
        row['econsub_recurrent_maintenance'] = 1
    if row.get('subsidies', '') == '1' and not row.get('econ2', '').startswith('02') and not row.get('econ2', '').startswith('01'):
        row['econsub_subsidies_to_production'] = 1
    if not any([row.get(f'econsub_{clean_col(cat)}', 0) == 1 for cat in econsub_categories if cat != "Other expenses"]):
        row['econsub_other_expenses'] = 1

    # ECON WIDE
    if row.get('econ2', '').startswith('01') and row.get('prog', '') != '2 Securite Sociale':
        row['econ_wage_bill'] = 1
    if row.get('econ1', '').startswith('Titre 2') and not row.get('econ2', '').startswith('10') and not row.get('prog','').startswith('2 Securite Sociale') and not row.get('admin1', '').startswith('05 '):
        row['econ_capital_expenditures'] = 1
    if row.get('econ2', '').startswith('02') and row.get('prog', '') != '2 Securite Sociale' and not row.get('admin1', '').startswith('05'):
        row['econ_goods_and_services'] = 1
    if row.get('subsidies', '') == '1' and not row.get('econ2', '').startswith('02') and not row.get('econ2', '').startswith('01'):
        row['econsubsidies'] = 1
    if row.get('econsub_social_assistance', 0) == 1 or row.get('econsub_pensions', 0) == 1:
        row['econ_social_benefits'] = 1
    if row.get('econ2', '').startswith('05'):
        row['econ_interest_on_debt'] = 1
    # Default: other expenses if none above
    if not any([row.get(f'econ_{clean_col(cat)}', 0) == 1 for cat in econ_categories if cat != 'Other expenses']):
        row['econ_other_expenses'] = 1

    return row

# Initialize all wide columns to 0
for cat in func_categories:
    df[f'func_{clean_col(cat)}'] = 0
for cat in funcsub_categories:
    df[f'funcsub_{clean_col(cat)}'] = 0
for cat in econsub_categories:
    df[f'econsub_{clean_col(cat)}'] = 0
for cat in econ_categories:
    df[f'econ_{clean_col(cat)}'] = 0

# Apply wide logic

df = df.apply(set_wide_columns, axis=1)
# Save or show the wide DataFrame
df = df.reset_index()
sdf = df.to_spark()
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.tun_boost_silver_test")

# COMMAND ----------

def collect_tags_vectorized(df, prefix, tags):
    wide_cols = [f'{prefix}_{clean_col(tag)}' for tag in tags]
    long_df = df[wide_cols]
    long_df = long_df.rename(columns={col: col.split(f"{prefix}_")[1].replace("_", " ") for col in long_df.columns})
    long_df['original_id'] = long_df.index
    # Melt to long format (pyspark.pandas does not support ignore_index)
    long_df = long_df.melt(var_name=f'{prefix}', value_name='is_tag', id_vars=['original_id'])
    # Add original index as a column
    long_df = long_df[long_df['is_tag'] == 1].drop(columns=['is_tag'])
    long_df[f'{prefix}'] = long_df[f'{prefix}'].str.capitalize()
    return long_df
    
# Apply for all categories
df['original_id'] = df.index
base_column_namses = [
    "original_id",
    "year",
    "ouvert",
    "ordonnance",
    "paye",
    "admin0_tmp",
    "admin1_tmp",
    "admin2_tmp",
    "geo1",
    "is_foreign"
]
base = df[base_column_namses]
base = base.merge(collect_tags_vectorized(df, 'econ', econ_categories), on='original_id')
base = base.merge(collect_tags_vectorized(df, 'econsub', econsub_categories), on='original_id')
base = base.merge(collect_tags_vectorized(df, 'func', func_categories), on='original_id')
base = base.merge(collect_tags_vectorized(df, 'funcsub', funcsub_categories), on='original_id')


# COMMAND ----------

renames = {"ouvert": "approved", "ordonnance": "revised", "paye": "executed", 'admin0_tmp': 'admin0', "admin1_tmp": "admin1", "admin2_tmp": "admin2", "original_id": "index"}
df = base.rename(columns=renames)
as_types = {'executed': float, "revised": float, "approved": float, "is_foreign": bool, "year": "Int32","index": "Int32"}
df = df.astype(as_types)
df["country_name"] = "Tunisia"
gold_column_namses = [
    "index",
    "country_name",
    "year",
    "approved",
    "revised",
    "executed",
    "admin0",
    "admin1",
    "admin2",
    "geo1",
    "is_foreign",
    "func",
    "funcsub",
    "econ",
    "econsub",
]
df=df[gold_column_namses]
database_name = "prd_mega.boost_intermediate"
sdf = df.to_spark()
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.tun_boost_gold_test")


# COMMAND ----------


