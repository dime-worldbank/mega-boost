# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
CCI_CSV_DIR = f'{WORKSPACE_DIR}/cci_csv'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

# COMMAND ----------

@dlt.table(name=f'quality_cci_bronze')
def quality_cci_bronze():
    ref_csv = f"{CCI_CSV_DIR}/Colombia/Approved.csv"
    ref_header = spark.read.text(ref_csv).first()[0]
    header_columns = ref_header.split(",")
    schema = StructType([StructField(header_columns[0], StringType())] +
                        [StructField(col, StringType()) for col in header_columns[1:]])
    
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .schema(schema)
      .load(f'{CCI_CSV_DIR}/*/')
      .withColumn("path_splitted", F.split(F.input_file_name(), "/"))
      .withColumn("approved_or_executed", F.regexp_replace(F.element_at(F.col("path_splitted"), -1), "\.csv", ""))
      .withColumn("country_name", F.regexp_replace(F.element_at(F.col("path_splitted"), -2), "%20", " "))
    )

# COMMAND ----------

@dlt.table(name=f'quality_total_silver')
def quality_total_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category') == 'Spending: Total Expenditures')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

# Exploratory for Manuel. Remove if they are not going to use
@dlt.table(name=f'quality_judiciary_silver')
def quality_judiciary_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category') == 'Spending in judiciary')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )
