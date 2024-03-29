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
@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'bfa_boost_bronze_1')
def boost_bronze_1():
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(f'{COUNTRY_MICRODATA_DIR}/BOOST.csv'))

    bronze1_selected_columns = ['YEAR','ADMIN1', 'GEO1', 'ECON1', 'ECON2', 'FUNCTION1','FUNCTION2', 'SECTOR2',  'APPROVED', 'MODIFIED', 'PAID'] 
    bronze1_dlt = bronze_df.select(*bronze1_selected_columns)
    bronze1_filtered_dlt = bronze1_dlt.na.drop("all")
    bronze1_filtered_dlt = bronze1_filtered_dlt.filter(col("YEAR") < 2017)
    return bronze1_filtered_dlt


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'bfa_boost_bronze_2')
def boost_bronze_2():
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(f'{COUNTRY_MICRODATA_DIR}/BOOST_.csv'))

    bronze2_selected_columns = ["YEAR", 'ADMIN1', "GEO1", "ECON1", "ECON2", "FUNCTION1", 'FUNCTION2', "APPROVED_1", "REVISED", "PAID"] 
    bronze2_dlt = bronze_df.select(*bronze2_selected_columns)
    bronze2_filtered_dlt = bronze2_dlt.na.drop("all")
    return bronze2_filtered_dlt

@dlt.table(name=f'bfa_boost_bronze')
def boost_bronze_combined():
    bronze1 = dlt.read(f'bfa_boost_bronze_1')
    bronze2 = dlt.read(f'bfa_boost_bronze_2')
    # Rename specific columns in bronze1 and bronze2
    bronze1 = bronze1.withColumnRenamed("MODIFIED", "REVISED")
    bronze2 = bronze2.withColumnRenamed("APPROVED_1", "APPROVED")
    bronze2 = bronze2.withColumn('SECTOR2', lit(None))

    # Concatenate the two DataFrames
    combined_bronze = bronze1.union(bronze2)
    return combined_bronze

@dlt.table(name=f'bfa_boost_silver')
def boost_silver():
    bronze = dlt.read(f'bfa_boost_bronze')
    silver_df = bronze.withColumn(
        'adm1_name_tmp',
        when(col("GEO1").isNotNull(),
             trim(initcap(regexp_replace(col("GEO1"), "[0-9\-]", " "))))
    ).withColumn(
        'adm1_name',
        when(col('adm1_name_tmp').isin('Central', 'Centrale'), 'Central Scope')
        .when(col('adm1_name_tmp') == 'Region Etrangere', 'Other')
        .when(col('adm1_name_tmp') == 'Est', 'Est Region Burkina Faso')
        .when(col('adm1_name_tmp') == 'Centre Sud', 'Centre Sud Region Burkina Faso')
        .otherwise(col('adm1_name_tmp'))
    ).withColumn(
        'admin0', lit('Central')
    ).withColumn(
        'admin1', lit('Central')
    ).withColumn(
        'admin2', col('ADMIN1')
    ).withColumn(
        'geo1',
        when(col('adm1_name_tmp').isin('Central', 'Centrale'), 'Central Scope')
        .when(col('adm1_name_tmp') == 'Region Etrangere', 'Other')
        .when(col('adm1_name_tmp') == 'Est', 'Est Region Burkina Faso')
        .when(col('adm1_name_tmp') == 'Centre Sud', 'Centre Sud Region Burkina Faso')
        .otherwise(col('adm1_name_tmp'))
    ).withColumn(
        'func_sub',
        when(col('FUNCTION2').startswith('033'), 'judiciary')
        .when(col('FUNCTION1').startswith('03') & (~col('FUNCTION2').startswith('033')), 'public safety')
        .when(col('FUNCTION2').startswith('072') | col('FUNCTION2').startswith('074'), 'primary and secondary health')
        .when(col('FUNCTION2').startswith('073'), 'tertiary and quaternary health')
        .when(col('FUNCTION2').startswith('091') | ((col('SECTOR2').startswith('42')) & (col('FUNCTION2').startswith('095') | col('FUNCTION2').startswith('096'))), 'primary education')
        .when((col('YEAR') < 2017) & (col('FUNCTION2').startswith('092')), 'secondary education')
    ).withColumn(
        'func',
        when(col('FUNCTION1').startswith('02'), 'Defence')
        .when(col('func_sub').isin('judiciary', 'public safety'), 'Public order and safety')
        .when(col('FUNCTION1').startswith('04'), 'Economic affairs')
        .when(col('FUNCTION1').startswith('05'), 'Environmental protection')
        .when(col('FUNCTION1').startswith('06'), 'Housing and community amenities')
        .when(col('FUNCTION1').startswith('07'), 'Health')
        .when(col('FUNCTION1').startswith('08'), 'Recreation, culture and religion')
        .when(col('FUNCTION1').startswith('09'), 'Education')
        .when(col('FUNCTION1').startswith('10'), 'Social protection')
        .otherwise('General public services')
    )
    
    return silver_df

@dlt.table(name=f'bfa_boost_gold')
def boost_gold():
    silver = dlt.read(f'bfa_boost_silver')
    gold_df = (silver
               .filter(
                   (col('ECON1') != '1 Amortissement, charge de la dette et depenses en attenuation des recettes ') |
                   ((col('ECON1') == '1 Amortissement, charge de la dette et depenses en attenuation des recettes ') &
                    (col('ECON2') == '65 Interets et frais financiers'))
                   )
               .withColumn('country_name', lit(COUNTRY))
               .select('country_name',
                       'adm1_name',
                       col('YEAR').alias('year').cast('int'),
                       col('APPROVED').alias('approved'),
                       col('REVISED').alias('revised'),
                       col('PAID').alias('executed'),
                       'admin0',
                       'admin1',
                       'admin2',
                       'geo1',
                       'func'
                       )
              )
    return gold_df

