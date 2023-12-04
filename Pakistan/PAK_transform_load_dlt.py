# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring
from pyspark.sql.types import StringType


# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Pakistan'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'pak_boost_bronze')
def boost_bronze():
    # Load the data from CSV
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(COUNTRY_MICRODATA_DIR))
    # convert the year column to int
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    # drop rows that have all null values
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df

@dlt.table(name=f'pak_boost_silver')
def boost_silver():
    return (dlt.read(f'pak_boost_bronze')
        .withColumn('adm1_name',                     
                    when(
                        col("Admin0") == "Federal", "Islamabad"
                    ).otherwise(
                        when(
                            col("Admin0") == "KP", "Khyber Paktunkhwa"
                        ).otherwise(col("Admin0"))
                    )       
        )
    )

@dlt.table(name=f'pak_boost_gold')
def boost_gold():
    return (dlt.read(f'pak_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'adm1_name',
                'year',
                'approved',
                'executed')
    )
