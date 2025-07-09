# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import pandas as pd
database_name = "prd_mega.boost_intermediate"

wide_df = spark.table(f'{database_name}.tun_boost_silver_test')

# COMMAND ----------

df = wide_df.toPandas()

# COMMAND ----------

df['paye'] = df['paye'].astype(float)
df.rename(columns={'paye': 'Overlapping Monetary Impact'}, inplace=True)

# COMMAND ----------

def get_overcounted_list(row, feature_columns_group):
    """
    Checks for overcounting within a group of hot-coded columns for a single row.
    Returns a list of the hot-coded items that are 'on' if overcounting occurs,
    otherwise an empty list.
    """
    active_features = [col.split('_', 1)[1] for col in feature_columns_group if row[col] == 1]

    if len(active_features) > 1:
        active_features.sort()
        return "|".join(active_features)
    else:
        return []

# COMMAND ----------

func_cols = [c for c in df.columns if c.startswith('func_') and not c.startswith('func_sub')]
#Func
df['Overcounted Items(func)'] = df.apply(lambda row: get_overcounted_list(row, func_cols), axis=1)
df["count_func"] = df[func_cols].sum(axis=1)

#Func_sub
func_sub_cols = [c for c in df.columns if c.startswith('func_sub')]
df['Overcounted Items(func_sub)'] = df.apply(lambda row: get_overcounted_list(row, func_sub_cols), axis=1)
df["count_func_sub"] = df[func_sub_cols].sum(axis=1)

#Econ
econ_cols = [c for c in df.columns if c.startswith('econ_') and not c.startswith('econ_sub')]
#Econ
df['Overcounted Items(econ)'] = df.apply(lambda row: get_overcounted_list(row, econ_cols), axis=1)
df["count_econ"] = df[econ_cols].sum(axis=1)

#Econ_sub
econ_sub_cols = [c for c in df.columns if c.startswith('econ_sub')]
df['Overcounted Items(econ_sub)'] = df.apply(lambda row: get_overcounted_list(row, econ_sub_cols), axis=1)
df["count_econ_sub"] = df[econ_sub_cols].sum(axis=1)


# COMMAND ----------

base_columns = [ 'year', 'type', 'gbo', 'admin1', 'admin2', 'econ1', 'econ2',
       'econ3', 'econ4', 'econ5', 'prog', 'sprog', 'geo1', 'fonds',
       'loi_de_finance', 'ouvert', 'ordonnance', 'Overcount Monetary Impact', 'delegue', 'roads',
       'air', 'wss', 'railroads', 'primary', 'secondary', 'soe', 'maintenance',
       'subsidies', 'admin0_tmp', 'admin1_tmp', 'admin2_tmp', 'is_foreign','original_id', 'econ',
       'econ_sub', 'func', 'func_sub', 'count_func', 'Overcounted Items(func)',
       'Overcounted Items(func_sub)', 'count_func_sub', 'Overcounted Items(econ)', 'count_econ',
       'Overcounted Items(econ_sub)', 'count_econ_sub']
df = df[base_columns]

# COMMAND ----------

func_overcounted = df[df['count_func'] > 1]
func_sub_overcounted = df[df['count_func_sub'] > 1]
econ_overcounted = df[df['count_econ'] > 1]
econ_sub_overcounted = df[df['count_econ_sub'] > 1]

# COMMAND ----------

func_result = func_overcounted.groupby('Overcounted Items(func)')['Overcount Monetary Impact'].sum().reset_index()
func_sub_result = func_sub_overcounted.groupby('Overcounted Items(func_sub)')['Overcount Monetary Impact'].sum().reset_index()
econ_result = econ_overcounted.groupby('Overcounted Items(econ)')['Overcount Monetary Impact'].sum().reset_index()
econ_sub_result = econ_sub_overcounted.groupby('Overcounted Items(econ_sub)')['Overcount Monetary Impact'].sum().reset_index()

# COMMAND ----------

sheet_name = "Overview"
blank_columns_separator = 1
OUTPUT_DIR = f"{TOP_DIR}/Workspace/output_excel"
excel_file_path = f"{OUTPUT_DIR}/overcounted.xlsx"

print(f"Successfully wrote DataFrames side-by-side to '{excel_file_path}' with {blank_columns_separator} blank column(s) as separator.")

# COMMAND ----------

import tempfile
import shutil
with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=True) as tmp:
    temp_path = tmp.name
    with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
        func_result.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=0, index=False)

        # Write the second DataFrame
        start_col_df2 = func_result.shape[1] + blank_columns_separator
        func_sub_result.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=start_col_df2, index=False)

        # Write the third DataFrame
        start_col_df3 = start_col_df2 + func_sub_result.shape[1] + blank_columns_separator
        econ_result.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=start_col_df3, index=False)

        # Write the fourth DataFrame
        start_col_df4 = start_col_df3 + econ_result.shape[1] + blank_columns_separator
        econ_sub_result.to_excel(writer, sheet_name=sheet_name, startrow=0, startcol=start_col_df4, index=False)

        func_overcounted.to_excel(writer, sheet_name="Overcounted Items(func)", index=False)
        func_sub_overcounted.to_excel(writer, sheet_name="Overcounted Items(func_sub)", index=False)
        econ_overcounted.to_excel(writer, sheet_name="Overcounted Items(econ)", index=False)   
        econ_sub_overcounted.to_excel(writer, sheet_name="Overcounted Items(econ_sub)")


    shutil.copy(temp_path, excel_file_path)


# COMMAND ----------


