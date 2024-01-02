# Databricks notebook source
from glob import glob
from pyspark.sql.types import StructType
import dlt
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, expr

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Burkina Faso'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

def infer_combined_schema(folder_path):
    # Get the list of CSV files in the folder
    csv_files = sorted(glob(f'/dbfs{folder_path}/*csv'))

    # Initialize an empty schema
    combined_schema = StructType([])

    # Infer schema separately for each CSV file and add fields to the combined schema
    for csv_file in csv_files:
        schema = spark.read.format("csv").options(**CSV_READ_OPTIONS).load(csv_file.replace('/dbfs', '')).schema
        for field in schema.fields:
            if field.name not in combined_schema.names:
                combined_schema = combined_schema.add(field.name, field.dataType, field.nullable, field.metadata)

    return combined_schema


@dlt.table(name=f'bfa_boost_bronze')
def boost_bronze():
    # Combine schemas using unionByName
    combined_schema = infer_combined_schema(COUNTRY_MICRODATA_DIR)
    print(f'Combined Schema Fields: {[(field.name, field.dataType) for field in combined_schema.fields]}')

    # Load the data from CSV with the specified schema
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .schema(combined_schema)
                 .load(COUNTRY_MICRODATA_DIR))
    
    return bronze_df

