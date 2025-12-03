# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import csv
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

# Add numeric columns to enforce as float64
for col in ["ORDONNANCER", "DOTATION_INITIALE", "DOTATION_FINALE"]:
    dtype_dict[col] = "float64"

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
df.to_csv(
    csv_file_path,
    index=False,
    encoding='utf-8',
    quoting=csv.QUOTE_NONNUMERIC
)

# COMMAND ----------

df_silver = df.copy()

# Year
df_silver["year"] = df_silver["YEAR"].astype("Int32")
df_silver.drop(columns=["YEAR"], inplace=True)

# Foreign funding flag
df_silver["is_foreign"] = ~df_silver["ADMIN5"].fillna("").str.startswith("FINANCEMENTS INTERNES")

# Administrative levels
df_silver["admin0"] = "Central"
df_silver['admin1'] = "Central Scope"

# Geographic levels
df_silver["geo0"] = np.where(
    df_silver["REGION"].notna() & (df_silver["REGION"] != "AUTRES REGIONS"),
    "Regional",
    "Central"
)

region_mapping = {
    "REGION CENTRALE": "Centrale",
    "REGION DE LA KARA": "Kara",
    "REGION MARITIME": "Maritime",
    "REGION DES PLATEAUX": "Plateaux",
    "REGION DES SAVANES": "Savanes",
    "AUTRES REGIONS DU MONDE": "Central Scope",
}
df_silver["geo1"] = np.where(
    df_silver["geo0"] == "Central", 
    "Central Scope",
    df_silver["REGION"].map(region_mapping).fillna("Central Scope")
)
assert df_silver["geo1"].nunique() == 6, (
    f"Expected 6 distinct values in geo1, found {df_silver['geo1'].nunique()}"
)
expected_geo1_values = sorted(list(region_mapping.values()))
actual_geo1_values = sorted(df_silver["geo1"].unique().tolist())
assert actual_geo1_values == expected_geo1_values, (
    f"Expected {expected_geo1_values}, found {actual_geo1_values}"
)

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

expected_func_values = sorted(list(func_map.values()))
actual_func_values = sorted(df_silver["func"].unique().tolist())
assert actual_func_values == expected_func_values, (
    f"Expected {expected_func_values}, found {actual_func_values}"
)

# Functional sub-classification
def map_func_sub(row):
    match row["CODE_FUNC1"]:
        case "03":
            match row["CODE_FUNC2"]:
                case "033":
                    return "Judiciary"
                case _:
                    return "Public Safety"
        case "09":
            match row["CODE_FUNC2"]:
                case "091":
                    return "Primary Education"
                case "092":
                    return "Secondary Education"
                case "093":
                    return "Post-Secondary Non-Tertiary Education"
                case "094":
                    return "Tertiary Education"
        case "04":
            if row["CODE_FUNC2"] == "042" or \
                (row["CODE_FUNC2"] == "048" and row["CODE_FUNC3"] == "0482"):
                return "Agriculture"
            elif row["CODE_FUNC2"] == "045":
                return "Transport"
            elif row["CODE_FUNC2"] == "046":
                return "Telecom"
            elif row["CODE_FUNC2"] == "043":
                return "Energy"
        case "06":
            if row["CODE_FUNC2"] == "063":
                return "Water Supply"
        # For "07"(Health) We don't have enough information to group by primary secondary tertiary

df_silver["func_sub"] = df_silver.apply(map_func_sub, axis=1)

# Functional sub-sub-classification
def map_func_sub_sub(row):
    match row["func_sub"]:
        case "Transport":
            match row["CODE_FUNC3"]:
                case "0451":
                    return "Road transport"
                case "0452":
                    return "Water transport"
                case "0453":
                    return "Railway transport"
                case "0454":
                    return "Air transport"
        case "Energy":
            if (row["CODE_FUNC3"] == '0435' or
                (row["CODE_FUNC3"] == '0436' and row["CODE_ECON3"] == '235' and row["CODE_ECON4"] =="235112" )):
                return "Electricity"
            elif row["CODE_FUNC3"] == '0432':
                return "Petroleum and natural gas"

df_silver["func_sub_sub"] = df_silver.apply(map_func_sub_sub, axis=1)

# Economic classification
def econ_map(row):
    match row["CODE_ECON1"]:
        case "1":
            return "Interest on debt"
        case "2":
            return "Wage bill"
        case "3":
            return "Goods and services"
        case "4":
            match row["CODE_ECON2"]:
                case "63":
                    econ3_other = (row["CODE_ECON3"] == "632") # SUBVENTIONS AUX ENTREPRISES PUBLIQUES
                    econ4_and_admin4_other = (row["CODE_ECON4"] == "633911" and
                        row["CODE_ADMIN4"] in [
                            "1131081332000", # DIRECTION DE LA PROMOTION ARTISTIQUE ET CULTURELLE (DPAC)
                            "1199000327001", # APPUI AUX MEDIAS
                            "1391080355000", # EDITOGO
                            "1139001299000", # GRATUITE DE SOINS DE LA FEMME ENCEINTE ET DU NOUVEAU NE
                        ]
                    )

                    if row["CODE_ADMIN4"] in [
                        "1199000174001", # ENEIGNEMENT CONFESSIONNEL PRIMAIRE
                        "1199000175001", # ENSEIGNEMENT CONFESSIONNEL DES 2E ET 3E DEGRES
                        "1199000175002", # SUBVENTION ADDITIONNELLE A L ENSEIGNEMENT CONFESSIONNEL
                    ]:
                        return "Wage bill"
                    elif econ3_other or econ4_and_admin4_other:
                        return "Other grants and transfers"
                    else:
                        return "Subsidies"
                case "64":
                    econ4_social = row["CODE_ECON4"] in [
                        "645111", # BOURSES ET ALLOCATIONS DE SECOURS AUX ELEVES ET ETUDIANTS A L’INTERIEUR
                        "645114", # FRAIS DE TRANSPORT DES ELEVES ET ETUDIANTS
                    ]
                    econ4_and_admin4_social = (
                        row["CODE_ECON4"] == '645911' # TRANSFERTS A D AUTRES ENTITES PUBLIQUES
                        and row["CODE_ADMIN4"] in [
                            "1139000960000", # TRANSFERES MONETAIRES AUX PARENTS D ELEVES
                            "1199000119002", # TRANSFERTS MONETAIRES
                            "1139000661000", # GRATUITE DE LA FEMME ENCEINTE ET DU NOUVEAU-NE
                            "1139000932000", # GRATUITE DES FRAIS D INSCRIPTION ET DE SCOLARITE
                            "1191080116001", # FONDS D APPUI AUX INITIATIVES ECONOMIQUES DES JEUNES (FAEIJ)
                            "1191080242016", # SUBVENTION A LA CESARIENE
                            "1199000119001", # SUBVENTION AUX CANTINES SCOLAIRES
                            "1199000242027", # PRISE EN CHARGE DES PVVIH/ARV
                    ])
                    admin4_social = row["CODE_ADMIN4"] == "1399000061000" # CAISSE DE RETRAITE DU TOGO (CRT)
                    if econ4_social or econ4_and_admin4_social or admin4_social:
                        return "Social benefits"
                    else:
                        return "Other grants and transfers"
        case "5":
            return "Capital expenditures"

df_silver["econ"] = df_silver.apply(econ_map, axis=1).fillna("Other expenses")

# Economic sub-classification
def map_econ_sub(row):
    match row["econ"]:
        case "Wage bill":
            if row["CODE_ECON3"] in ["661", "665"] :
                return "Basic Wages"
            if row["CODE_ECON3"] == "663":
                return "Allowances"
        case "Goods and services":
            if row["CODE_ECON3"] == "614":
                return "Recurrent Maintenance"
        case "Capital expenditures":
            if row.get("is_foreign"):
                return "Capital Expenditure (foreign spending)"
            if "REHABILITATION" in row["ADMIN4"]:
                return "Capital Maintenance"
        case "Subsidies": 
            if row["CODE_ECON3"] in ["633", "639"]:
                return "Subsidies to Production"
        case "Social benefits":
            if row["CODE_ADMIN4"] == "1399000061000": # CAISSE DE RETRAITE DU TOGO (CRT)
                return "Pensions"
            else:
                return "Social Assistance"
df_silver["econ_sub"] = df_silver.apply(map_econ_sub, axis=1)

# Save silver table to Unity Catalog if running in Databricks, else export to CSV
if IS_DATABRICKS:
    sdf = spark.createDataFrame(df_silver)
    sdf.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable("prd_mega.boost_intermediate.tgo_2021_onward_boost_silver")
else:
    # TODO: directly write to relational database when credentials are available
    df_silver.to_csv(
        f"{output_dir}tgo_2021_onward_boost_silver.csv",
        index=False,
        encoding='utf-8',
        quoting=csv.QUOTE_NONNUMERIC
    )

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
df_gold['country_name'] = 'Togo'

if IS_DATABRICKS:
    sdf_gold = spark.createDataFrame(df_gold)
    sdf_gold.write.mode("overwrite").option("overwriteSchema", "true")\
        .saveAsTable("prd_mega.boost_intermediate.tgo_boost_gold")
else:
    # TODO: directly write to relational database when credentials are available
    df_gold.to_csv(
        f"{output_dir}tgo_boost_gold.csv",
        index=False,
        encoding='utf-8',
        quoting=csv.QUOTE_NONNUMERIC
    )
