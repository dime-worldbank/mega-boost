# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

COUNTRY = 'Congo'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

disaggregated_data_sheets = ['BOOST 2012-2021']

for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    df = pd.read_excel(filename, sheet_name=sheet, header=0)

    header = [col_name for col_name in df.columns if normalize_cell(is_named_column(col_name))]
    df = df[header]
        
    # Normalize cells
    df = df.applymap(normalize_cell)

    df.to_csv(csv_file_path, index=False, encoding='utf-8')

# COMMAND ----------

df

# COMMAND ----------


