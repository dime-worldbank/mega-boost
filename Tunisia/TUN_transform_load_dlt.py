# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace, regexp_extract, substring

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/Tunisia'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'tun_boost_bronze')
def boost_bronze():
    bronze_df = (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(COUNTRY_MICRODATA_DIR)
    )
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name=f'tun_boost_silver')
def boost_silver():
    return (dlt.read(f'tun_boost_bronze')
            # Central scope not explicitly defined. Only other entries are 00: not disbursed, 99: interest 98: unnamed
            # If null then attach to central scope
        .withColumn('adm1_name', 
            when(regexp_extract(col("GEO1"), r'^[1-8]', 0).isNotNull(), trim(regexp_replace(col("GEO1"), "\d+", "")))
            .when(col("GEO1").startswith("0") | col("GEO1").startswith("9"), "Other")
            .when(col("GEO1").isNull(), "Central Scope"))
        ).withColumn(
    'func_sub',
    when(col('ADMIN1').startswith('07'), 'judiciary')
    .when((col('ADMIN1').startswith('06')) & (col('ADMIN2').startswith('07')), 'public safety')
    .when(col('Primary')==1, 'primary education')
    .when(col('Secondary')==1, 'secondary education')
    .when(col("ADMIN2").startswith("02") | col("ADMIN2").startswith("30") | col("ADMIN2").startswith("33"),  "tertiary education")
    # No breakdown in health expenditure into primary, secondary and tertiary       
    ).withColumn(
    'func',
    when((col('ADMIN1').startswith('09') | col('ADMIN1').startswith('06')), 'Defense')
    .when(col("func_sub").isin("judiciary", "public safety") , "Public order and safety")
    .when(col('ADMIN2').startswith('21'), 'Environmental protection')
    .when(col('ADMIN2').startswith('27') | col('ADMIN2').startswith('34'), 'Health')
    .when(substring(col("ADMIN2"), 1, 2).isin('04 29 30 33 37 39 40'.split()), 'Education')
    .when(col('ADMIN1').startswith('05'), 'Social protection')
    # missing classification into 
    #General public services, 
    #Economic affairs, 
    #Housing & community amenities, and 
    #Recreation, culture & religion
    ).withColumn('is_transfer', lit(False))
    

@dlt.table(name=f'tun_boost_gold')
def boost_gold():
    return (dlt.read(f'tun_boost_silver')
            .filter(~(col('ECON2').startswith('10')))
            .withColumn('country_name', lit('Tunisia')) 
            .select('country_name',
                    'adm1_name',
                    col('YEAR').alias('year'),
                    col('OUVERT').alias('approved'),
                    col('ORDONNANCE').alias('revised'),
                    col('PAYE').alias('executed'),
                    'is_transfer',
                    'func'
                    )
    )
