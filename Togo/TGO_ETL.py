# Databricks notebook source
import os
from pathlib import Path
import numpy as np
import pandas as pd
import openpyxl

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# COMMAND ----------


if IS_DATABRICKS:
    TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
    INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
    COUNTRY = 'Togo'
    filename = f"{INPUT_DIR}/{COUNTRY} BOOST.xlsx"
    sheet_name = "2021_A_2025"
else: 
    filename = os.environ.get('INPUT_FILE_NAME') or input("Enter the path to the input Excel file (e.g. /path/to/input.xlsx): ").strip()
    sheet_name = os.environ.get('INPUT_SHEET_NAME') or input("Enter the name of the sheet in the Excel file (e.g. data): ").strip()

# Read header rows to extract CODE_* columns for type enforcement
preview_df = pd.read_excel(filename, sheet_name=sheet_name, nrows=0)
code_cols = [col for col in preview_df.columns if col.startswith("CODE_")]
dtype_dict = {col: str for col in code_cols}

# Read full sheet with specified dtypes
df = pd.read_excel(filename, sheet_name=sheet_name, dtype=dtype_dict)
df

# COMMAND ----------

df = df[[col for col in df.columns if col and not str(col).startswith("Unnamed")]]
df.columns = [col.strip() for col in df.columns]
df.dropna(how='all', inplace=True)
df

# COMMAND ----------

if IS_DATABRICKS:
    WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
    microdata_csv_dir = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'
    Path(microdata_csv_dir).mkdir(parents=True, exist_ok=True)
    csv_file_path = f'{microdata_csv_dir}/{sheet_name}.csv'
else:
    output_dir = os.environ.get('OUTPUT_DIR') or input("Enter the output directory (e.g. /path/to/output/): ").strip()
    csv_file_path = f'{output_dir}{sheet_name}.csv'

# Save bronze data to CSV
df.to_csv(csv_file_path, index=False, encoding='utf-8')

# COMMAND ----------

df_silver = df.copy()

# Year
df_silver["year"] = df_silver["YEAR"].astype("Int64")
df_silver.drop(columns=["YEAR"], inplace=True)

# Foreign funding flag
df_silver["is_foreign"] = ~df_silver["ADMIN5"].fillna("").str.startswith("FINANCEMENTS INTERNES")

# Administrative levels
df_silver["admin0"] = "Central"
df_silver["admin1"] = "Central Scope"

# Geographic levels
df_silver["geo0"] = np.where(
    df_silver["REGION"].notna() & (df_silver["REGION"] != "AUTRES REGIONS"),
    "Regional",
    "Central"
)
df_silver["geo1"] = np.where(df_silver["geo0"] == "Central", "Central Scope", df_silver["REGION"])
df_silver["geo2"] = df_silver["PREFECTURE"]

# Functional classification
func_map = {
    "01": "General public services",
    "02": "Defence",
    "03": "Public order and safety",
    "04": "Economic affairs",
    "05": "Environmental protection",
    "06": "Housing and community amenities",
    "07": "Health",
    "08": "Recreation, culture and religion",
    "09": "Education",
    "10": "Social protection",
}
df_silver["func"] = df_silver["CODE_FUNC1"].map(func_map)

# Functional sub-classification
def map_func_sub(row):
    if row["CODE_FUNC1"] == "03":
        return "Judiciary" if row["CODE_FUNC2"] == "033" else "Public Safety"
    if row["CODE_FUNC1"] == "09":
        if row["CODE_FUNC2"] == "091":
            return "Primary Education"
        if row["CODE_FUNC2"] == "092":
            return "Secondary Education"
        if row["CODE_FUNC2"] == "094":
            return "Tertiary Education"
    return ""
# TODO: "07" "Health"
# TODO: "04" "Economic affairs" 
# Reference Togo BOOST CCI Executed sheet for mapping & see standardized func_sub values: https://github.com/dime-worldbank/mega-boost/blob/main/quality/transform_load_dlt.py#L134-L149 

df_silver["func_sub"] = df_silver.apply(map_func_sub, axis=1)

# Economic classification
econ_map = {
    "1": "Interest on debt",
    "2": "Wage bill",
    "3": "Goods and services",
    "5": "Capital expenditures"
}
df_silver["econ"] = df_silver["CODE_ECON1"].map(econ_map).fillna("Other expenses")

# TODO: "Subsidies", 
# TODO: "Social benefits", 
# TODO: "Other grants and transfers", 
# See how it's done for past years in Togo BOOST CCI Executed sheet

# Economic sub-classification
def map_econ_sub(row):
    if row["CODE_ECON1"] == "2":
        if row["CODE_FUNC2"] in ["661", "665"]:
            return "Basic Wages"
        if row["CODE_FUNC2"] == "663":
            return "Allowances"
        if row["CODE_FUNC2"] in ["664", "666"]:
            return "Social Benefits (pension contributions)"
    elif row["CODE_ECON1"] == "3":
        if row["CODE_ECON3"] == "614":
            return "Recurrent Maintenance"
        # TODO: "Basic Services", 
        # TODO: "Employment Contracts"
    elif row["CODE_ECON1"] == "5":
        if row.get("is_foreign"):
            return "Capital Expenditure (foreign spending)"
        # TODO: "Capital Maintenance"
    return ""

# TODO: within Subsideies: "Subsidies to Production"
# TODO: within Social benefits: "Social Assistance", "Pensions", "Other Social Benefits"

df_silver["econ_sub"] = df_silver.apply(map_econ_sub, axis=1)

# Save silver table to Unity Catalog if running in Databricks, else export to CSV
if IS_DATABRICKS:
    sdf = spark.createDataFrame(df_silver)
    sdf.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable("prd_mega.sboost4.tgo_2021_onward_boost_silver")
else:
    # TODO: directly write to relational database when credentials are available
    df_silver.to_csv(f"{output_dir}tgo_2021_onward_boost_silver.csv", index=False)

# COMMAND ----------

# Select and rename columns for the gold table
df_gold = df_silver[[
    'year',
    'admin0',
    'admin1',
    'ADMIN2',
    'geo0',
    'geo1',
    'func',
    'func_sub',
    'econ',
    'econ_sub',
    'is_foreign',
    'ORDONNANCER',
    'DOTATION_INITIALE',
    'DOTATION_FINALE'
]].rename(columns={
    'ADMIN2': 'admin2',
    'ORDONNANCER': 'executed',
    'DOTATION_INITIALE': 'approved',
    'DOTATION_FINALE': 'revised'
})

if IS_DATABRICKS:
    sdf_gold = spark.createDataFrame(df_gold)
    sdf_gold.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable("prd_mega.sboost4.tgo_boost_gold")
else:
    # TODO: directly write to relational database when credentials are available
    df_gold.to_csv(f"{output_dir}tgo_boost_gold.csv", index=False, encoding='utf-8')
