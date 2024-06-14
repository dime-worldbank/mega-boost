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
    ref_csv = f"{CCI_CSV_DIR}/COL/Approved.csv"
    ref_header = spark.read.text(ref_csv).first()[0]
    header_columns = ref_header.split(",")
    schema = StructType([StructField(header_columns[0], StringType())] +
                        [StructField(col, StringType()) for col in header_columns[1:]])
    countries = spark.table('indicator.country').select('country_code', 'country_name')
    
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .schema(schema)
      .load(f'{CCI_CSV_DIR}/*/')
      .withColumn("path_splitted", F.split(F.input_file_name(), "/"))
      .withColumn("approved_or_executed", F.regexp_replace(F.element_at(F.col("path_splitted"), -1), "\.csv", ""))
      .withColumn("country_code", F.element_at(F.col("path_splitted"), -2))
      .join(countries, on=["country_code"], how="left")
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

@dlt.table(name=f'quality_functional_silver')
def quality_functional_silver():
    udf_capitalize  = F.udf(lambda x: str(x).capitalize(), StringType())
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code').startswith('EXP_FUNC_'))
        .withColumn('func',
            F.when(
                F.col("category_code") == 'EXP_FUNC_GEN_PUB_SER_EXE' , "General public services"
            ).when(
                F.col("category_code") == 'EXP_FUNC_DEF_EXE' , "Defence"
            ).when(
                F.col("category_code") == 'EXP_FUNC_PUB_ORD_SAF_EXE' , "Public order and safety"
            ).when(
                F.col("category_code") == 'EXP_FUNC_ECO_REL_EXE' , "Economic affairs"
            ).when(
                F.col("category_code") == 'EXP_FUNC_ENV_PRO_EXE' , "Environmental protection"
            ).when(
                F.col("category_code") == 'EXP_FUNC_HOU_EXE' , "Housing and community amenities"
            ).when(
                F.col("category_code") == 'EXP_FUNC_HEA_EXE' , "Health"
            ).when(
                F.col("category_code") == 'EXP_FUNC_REV_CUS_EXC_EXE' , "Recreation, culture and religion"
            ).when(
                F.col("category_code") == 'EXP_FUNC_EDU_EXE' , "Education"
            ).when(
                F.col("category_code") == 'EXP_FUNC_SOC_PRO_EXE' , "Social protection"
            )
        )
        .filter(F.col('func').isNotNull())
        .melt(ids=["country_name", "approved_or_executed", "func"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f'quality_economic_silver')
def quality_economic_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code').startswith('EXP_ECON_'))
        .withColumn('econ',
            F.when(
                F.col("category_code") == 'EXP_ECON_WAG_BIL_EXE' , "Wage bill"
            ).when(
                F.col("category_code") == 'EXP_ECON_CAP_EXP_EXE' , "Capital expenditures"
            ).when(
                F.col("category_code") == 'EXP_ECON_USE_GOO_SER_EXE' , "Goods and services"
            ).when(
                F.col("category_code") == 'EXP_ECON_SUB_EXE' , "Subsidies"
            ).when(
                F.col("category_code") == 'EXP_ECON_SOC_BEN_EXE' , "Social benefits"
            ).when(
                F.col("category_code") == 'EXP_ECON_OTH_GRA_EXE' , "Other grants and transfers"
            ).when(
                F.col("category_code") == 'EXP_ECON_OTH_EXP_EXE' , "Other expenses"
            ).when(
                F.col("category_code") == 'EXP_ECON_INT_DEB_EXE' , "Interest on debt"
            )
        )
        .filter(F.col('econ').isNotNull())
        .melt(ids=["country_name", "approved_or_executed", "econ"], 
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

# COMMAND ----------


@dlt.table(name=f'quality_total_subnat_silver')
def quality_total_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code') == 'EXP_ECON_SBN_TOT_SPE_EXE')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )
