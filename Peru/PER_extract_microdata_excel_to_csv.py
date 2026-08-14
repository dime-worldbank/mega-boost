# Databricks notebook source
# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import pandas as pd

COUNTRY = 'Peru'
START_YEAR = 2006
END_YEAR = 2023

microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

required_columns = [
    "year",
    "admin1",
    "econ1",
    "econ2",
    "econ3",
    "econ4",
    "function1",
    "function2",
    "function3",
    "source_fin1",
    "source_fin2",
    "monto_pia",
    "monto_devengado",
]

disaggregated_data_sheets = ['Raw']
for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'

    # The workbook's Raw sheet has pivot/helper columns to the right. Keep only
    # the microdata input that later pipeline steps should read.
    df = pd.read_excel(
        filename,
        sheet_name=sheet,
        header=0,
        usecols="A:M",
        na_values=[".."],
    )

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [str(col).strip() for col in header]

    # Normalize cells
    df = df.applymap(normalize_cell)

    # Remove rows where all values are null
    df = df.dropna(how='all')

    missing_columns = sorted(set(required_columns) - set(df.columns))
    assert not missing_columns, f"Missing required columns in {sheet}: {missing_columns}"

    df = df[required_columns]
    df = df[df['year'].notna()]
    df['year'] = df['year'].astype(int)

    existing_years = set(df['year'].unique())
    expected_years = set(range(START_YEAR, END_YEAR + 1))
    missing_years = sorted(expected_years - existing_years)
    assert not missing_years, f"Missing expected Peru expenditure years: {missing_years}"

    assert len(df) >= 500000, f"Expected at least 500,000 Peru raw rows, got {len(df)}"

    # Write to CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

