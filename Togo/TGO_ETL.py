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
df_silver['admin1'] = df_silver['TYPE_BUDGET'].fillna("")

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
    if row["CODE_FUNC1"]=="04":
        if row["CODE_FUNC2"] == "042" or (row["CODE_FUNC2"] == "048" and row["CODE_FUNC3"] == "0482" ) :
            return "Agriculture"
        if row["CODE_FUNC2"] == "045":
            if row["CODE_FUNC3"] == "0451":
                return "Spending in roads"
            if row["CODE_FUNC3"] == "0453":
                return "Spending in railroads"
            if row["CODE_FUNC2"] == "045" and row["CODE_FUNC3"] == "0452":
                return "Spending in water transport"
            if row["CODE_FUNC2"] == "045" and row["CODE_FUNC3"] == "0454":
             return "Spending in air transport"
        if row["CODE_FUNC3"] == "0432" and row["CODE_ECON3"] in ["639","633"]:
                return "Spending in subsidies in energy"     
        if  row["CODE_FUNC3"] == "0436" and row["CODE_ECON3"] == "235" and row["CODE_ECON4"] == "235112":        
            return "Spending in energy (power)"
        if (row["CODE_FUNC2"] == "043" and row["CODE_FUNC3"] == "0435"):
            return "Spending in energy (power)"
        if (row["CODE_FUNC2"] == "043" and row["CODE_FUNC3"] == "0432"):
            return "Spending in energy (oil & gas)"
        # add for Spending in hydropower
        if row["CODE_FUNC2"] == "046":
            return "Spending in telecoms"
    return ""

# TODO: ❌"07" "Health" "Health" We don't have enough information to group by (primary secondary tertiary) 
# TODO: ✅"04" "Economic affairs" 
# Reference Togo BOOST CCI Executed sheet for mapping & see standardized func_sub values: https://github.com/dime-worldbank/mega-boost/blob/main/quality/transform_load_dlt.py#L134-L149 

df_silver["func_sub"] = df_silver.apply(map_func_sub, axis=1)

# Economic classification
#I deleted the first econ_map because i created new one in order to take in account the conditions
#econ_map = {
#    "1": "Interest on debt",
#    "2": "Wage bill",
#    "3": "Goods and services",
#    "5": "Capital expenditures"
#}

def econ_map(row):
    if row["CODE_ECON1"] == "1":
        return "Interest on debt"
    if row["CODE_ECON1"] == "2" and row["CODE_ECON4"] == "633112" and  row["CODE_ADMIN4"] in ["1199000174001", "1199000175001", "1199000175002"]:
        return "Wage bill"
    if row["CODE_ECON1"] == "3":
        return "Goods and services"
    if row["CODE_ECON1"] == "5" and row["CODE_ECON2"] not in ["26"]:
        return "Capital expenditures"
    if row["CODE_ECON1"] == "4" and row["CODE_ECON2"] == "63" and row["CODE_ECON3"] in ["633","639"] : 
        if row["CODE_ECON4"] == "633112" and row["CODE_ADMIN4"] in [
        "1199000174001", "1199000175001", "1199000175002"]:
            return None
        if row["CODE_ECON4"] == "633911" and row["CODE_ADMIN4"]  in ["1131081332000", "1199000327001"]:
            return  None
        if row["CODE_ECON4"] == "639111" and row["CODE_ADMIN4"]  in ["1391080355000"]:
            return None
        if row["CODE_ECON4"] == "639911" and row["CODE_ADMIN4"]  in ["1139001299000"]:
            return None
        return "Subsidies"      
    if row["CODE_ECON3"] in ["664","666"] :
        return "Social assistance"
    if row["CODE_ECON3"]=="645" and row["CODE_ECON4"] in ["645111","645114"]:
        return "Social assistance"
    if row["CODE_ECON4"]=="645911" and row["CODE_ADMIN4"] in ["1139000960000","1199000119002"]:
        return "Social assistance"
    if row["CODE_ECON4"]=="649911" and row["CODE_ADMIN4"] in ["1139000661000","1191080116001","1191080116001","1191080242016","1199000119001","1199000242027"]:
        return "Social assistance"
    if row["CODE_ECON2"] == "64": 
        if (row["CODE_ECON4"] == "633911" and row["CODE_ADMIN4"]  in ["1131081332000", "1199000327001"]):
             return "Other grants and transfers"
        if row["CODE_ECON4"]=="649911" and row["CODE_ADMIN4"] in ["1139000661000","1191080116001","1191080116001","1191080242016","1199000119001","1199000242027"]:
            return None
        if row["CODE_ECON4"]=="645911" and row["CODE_ADMIN4"] in ["1139000960000","1199000119002"]:
            return None
        if row["CODE_ECON3"]=="645" and row["CODE_ECON4"] in ["645111","645114"]:
            return None
        return "Other grants and transfers"
    if (row["CODE_ECON4"] == "633911" and row["CODE_ADMIN4"] in ["1131081332000", "1199000327001","1391080355000"] ):
            return "Other grants and transfers"
    return "Other expenses"
df_silver["econ"] = df_silver.apply(econ_map, axis=1)

#df_silver["econ"] = df_silver["CODE_ECON1"].map(econ_map).fillna("Other expenses")

# TODO: ✅"Subsidies", 
# TODO: ✅"Social benefits" : We're focused on Social Assistance
# TODO: ✅"Other grants and transfers", 
# See how it's done for past years in Togo BOOST CCI Executed sheet

# Economic sub-classification
def map_econ_sub(row):
    if row["CODE_ECON1"] == "2":
        if row["CODE_ECON3"] in ["661", "665"] :
            return "Basic wages"
        if row["CODE_ECON3"] == "663":
            return "Allowances"
    elif row["CODE_ECON1"] == "3":
        if row["CODE_ECON3"] == "614":
            return "Recurrent Maintenance"
        # TODO: ✅"Basic Services", 
        if row["CODE_ECON2"] in ["61","62"] and row["CODE_ECON3"]!= "614":
            return "Basic Services"
        # TODO:❌ "Employment Contracts" We don't know how to include in this category
    elif row["CODE_ECON1"] == "5":
        if row.get("is_foreign"):
            return "Capital Expenditure (foreign spending)"
        # TODO: "Capital Maintenance" ❌  It's not clear in the CCI how to calculate it

# TODO: within Subsideies: "Subsidies to Production"
    elif row["econ"] == "Subsidies": 
        return "Subsidies to Production"
# TODO:✅ within Social benefits: "Social Assistance", "Pensions", "Other Social Benefits": We've only focused on social assistance
    elif row["econ"] == "Social assistance":
        return "Social assistance"
    return ""
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
