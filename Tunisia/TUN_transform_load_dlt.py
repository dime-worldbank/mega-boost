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
            .withColumn('adm1_name', 
                when(col("GEO1").isNull(), "Central Scope")
                .when(col("GEO1").startswith("0") | col("GEO1").startswith("9"), "Other")
                .when(col("GEO1").rlike('^[1-8]'), trim(regexp_replace(col("GEO1"), '^[1-8]+\\s*', '')))
                )
        ).withColumn(
        'admin1', trim(regexp_replace(col("ADMIN2"), '^[0-9\\s]*', ''))
        ).withColumn(
    'func_sub',
        when((col('ADMIN1').startswith('06')) & (col('ADMIN2').startswith('07')), 'public safety')
        .when(col('ADMIN1').startswith('07'), 'judiciary')
        .when(substring(col("ADMIN2"), 1, 2).isin('04 30 33'.split()), 'tertiary education')
        .when((col("ADMIN2").startswith("16") | col("ADMIN2").startswith("17")), 'agriculture')
        .when(col('ADMIN1').startswith('18') , 'telecom')
        .when(((col('Roads') ==1) | (col('railroads') == 1) | (col('Air') == 1) | (col('WSS')==1)), 'transport')
    ).withColumn(
    'func',
        when((col('ADMIN1').startswith('09') | col('ADMIN1').startswith('06')), 'Defence')
        .when(col("func_sub").isin('public safety', 'judiciary') , "Public order and safety")
        .when(col('ADMIN2').startswith('21'), 'Environmental protection')
        .when(col('ADMIN2').startswith('27') | col('ADMIN2').startswith('34'), 'Health')
        .when(col('ADMIN1').startswith('05'), 'Social protection')
        .when(substring(col("ADMIN2"), 1, 2).isin('04 29 30 33 37 39 40'.split()), 'Education')
        .when(col('WSS')==1, 'Housing and community amenities')
        .when(substring(col("ADMIN1"), 1, 2).isin('19 10 20'.split()), 'Recreation, culture and religion')
        .when(col("func_sub").isin('agriculture', 'transport', 'telecom') , "Economic affairs")
        .otherwise('General public services')
    ).withColumn('is_transfer', lit(False))

@dlt.table(name=f'tun_boost_gold')
def boost_gold():
    return (dlt.read(f'tun_boost_silver')
            #.filter(~((col('Maintenance')==1)& (col('ECON1').startswith('Titre 2'))))
            .withColumn('country_name', lit('Tunisia')) 
            .select('country_name',
                    'adm1_name',
                    col('YEAR').alias('year'),
                    col('OUVERT').alias('approved'),
                    col('ORDONNANCE').alias('revised'),
                    col('PAYE').alias('executed'),
                    col('admin1'),
                    'is_transfer',
                    'func'
                    )
    )
