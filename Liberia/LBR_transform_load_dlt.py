# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import (
    substring, col, lit, when, udf, trim, regexp_replace, initcap, concat, lower, create_map, coalesce
)

# Note: DLT requires the path to not start with /dbfs
TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Liberia'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

region_mapping = {
    
}

region_mapping_list = []
for key, val in region_mapping.items():
    region_mapping_list.extend([lit(key), lit(val)])
region_mapping_expr = create_map(region_mapping_list)


@dlt.table(name='lbr_boost_bronze')
def boost_bronze():
    return (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/Data.csv'))


@dlt.table(name='lbr_boost_silver')
def boost_silver():
    df = (dlt.read('lib_boost_bronze')
        .withColumn("econ0", coalesce(col("econ0").cast("string"), lit("")))
        .withColumn("econ1", coalesce(col("econ1").cast("string"), lit("")))
        .withColumn("econ2", coalesce(col("econ2").cast("string"), lit("")))
        .withColumn("econ3", coalesce(col("econ3").cast("string"), lit("")))
        .withColumn("econ4", coalesce(col("econ4").cast("string"), lit("")))
        .withColumn("Region", coalesce(col("Region").cast("string"), lit("")))
        .filter((col('econ0') == '2 Gasto') & (lower(col('econ1')) != 'gastos por actividades de financiacion'))
        .withColumnRenamed('accrued', 'executed')
        .withColumnRenamed('Servicio', 'service')
        .withColumnRenamed('Region', 'region'))

    return df


@dlt.table(name=f'lbr_boost_gold')
def boost_gold():
    return (dlt.read(f'lib_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .filter(col('year') > 2008)
        .select('country_name',
                col('year').cast("integer"),
                'approved',
                col('modified').alias('revised'),
                'executed',
                'geo1',
                col('admin0_tmp').alias('admin0'),
                col('admin1_tmp').alias('admin1'),
                'admin2',
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

