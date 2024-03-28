# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, expr
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
            when(col("Admin0") == "Federal", "Central Scope")
            .otherwise(
                when(col("Admin0") == "KP", "Khyber Pakhtunkhwa")
                .otherwise(col("Admin0")))
        ).withColumn(
            'admin0_tmp', 
            when(col('Admin0')=='Federal', 'Central')
            .otherwise('Regional')
        ).withColumn(
            'admin1_tmp', # since admin1 already exists in the raw data
            when(col('admin0_tmp')=='Central', 'Central')
            .when(col("Admin0") == "KP", 'Khyber Pakhtunkhwa')
            .otherwise(col('Admin0'))
        ).withColumn(
            'admin2',
            initcap(trim(expr("substring(admin1, instr(admin1, '-') + 1)")))
        ).withColumn(
            'geo1', 
            when(col('admin0_tmp')=='Central', 'Central Scope') # since we don't have geo tagged spending information
            .otherwise(col('admin1_tmp'))
        ).withColumn(
            'func_sub',
            when(col('func2').startswith('031'), 'judiciary')
            .when((col('func1').startswith('03') & (~col('func2').startswith('031'))), 'public order')
            .when(col('func2').startswith('091'), 'primary education')
            .when(col('func2').startswith('092'), 'secondary education')
            .when(col('func2').startswith('093'), 'tertiary education')
        ).withColumn(
            'func',
            when((col('func1') == '0') & col('admin1').startswith('H01'), 'Health')
            .when((col('func1') == '0') & col('admin1').startswith('E01'), 'Education')
            .when(col('func1').startswith('01'), 'General public services')
            .when(col('func1').startswith('02'), 'Defence')
            .when(col('func_sub').isin('judiciary', 'public order'), 'Public order and safety')
            .when(col('func1').startswith('04'), 'Economic affairs')
            .when(col('func1').startswith('05'), 'Environmental protection')
            .when(col('func1').startswith('06'), 'Housing and community amenities')
            .when(col('func1').startswith('07'), 'Health')
            .when(col('func1').startswith('08'), 'Recreation, culture and religion')
            .when(col('func1').startswith('09'), 'Education')
            .when(col('func1').startswith('10'), 'Social protection')
            .otherwise(lit("Other")) # TODO: func1 (blank) is currently marked as 'Other'
        ).withColumn('is_transfer', lit(False))
    )

@dlt.table(name=f'pak_boost_gold')
def boost_gold():
    return (dlt.read(f'pak_boost_silver')
            .filter(~((col('econ1')== "A08 Loans and Advances")|
                    (col('econ1')=="A10 Principal Repayments of Loans")))
            .withColumn('country_name', lit(COUNTRY))
            .select('country_name',
                    'adm1_name',
                    'year',
                    'approved',
                    expr("CAST(NULL AS DOUBLE) as revised"),
                    'executed',
                    col('admin0_tmp').alias('admin0'),
                    col('admin1_tmp').alias('admin1'),
                    'admin2',
                    'geo1',
                    'is_transfer',
                    'func',
                    'func_sub')
    )
