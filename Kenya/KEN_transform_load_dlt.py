# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring
from pyspark.sql.types import StringType


# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Kenya'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'ken_boost_bronze')
def boost_bronze():
    # Load the data from CSV
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(COUNTRY_MICRODATA_DIR))
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name=f'ken_boost_silver')
def your_bronze_table_silver():
    return (dlt.read(f'ken_boost_bronze')
        .withColumn('adm1_name', 
                    when(col("Counties_Geo2").isNotNull(),
                         when(lower(col("Counties_Geo2")).like("%county%"),
                              initcap(trim(regexp_replace(regexp_replace(col("Counties_Geo2"), "\d+", ""), "County", "")))
                         )
                         .when(lower(col("Counties_Geo2")).like("%nation%"), 'Central Scope')
                         .otherwise('Other') # When 0
                    )
                    .otherwise('Other') # When Null
        )
        .withColumn('year', substring(col('Year'), 1, 4).cast('int'))
    )
    
@dlt.table(name=f'ken_boost_gold')
def your_gold_table():
    return (dlt.read(f'ken_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'adm1_name',
                'year',
                col('Initial_Budget_Printed_Estimate').alias('approved'),
                col('Final_Budget_Approved_Estimate').alias('revised'),
                col('`Final_Expenditure_Total_Payment_Comm.`').alias('executed'))
    )
