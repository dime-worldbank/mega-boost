# Databricks notebook source
# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf, trim, regexp_replace, initcap, concat
from pyspark.sql.types import StringType

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Uruguay'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'ury_boost_bronze')
def boost_bronze():
    # Load the data from CSV
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}')
    )

@dlt.table(name=f'ury_boost_silver')
def boost_silver():
    return (dlt.read(f'ury_boost_bronze')
            .withColumn('admin0', lit('Central')) # No subnational data available
            .withColumn('geo1', lit('Central')) # No subnational data available
            .withColumn('is_foreign', col('SOURCE_FIN1').startswith('20 '))
            .withColumn('func_sub',
                        when(col('func1').startswith('01 '), 'judiciary')
                        .when(col('func1').startswith('14 '), 'public safety')
                        .when(col("admin1").startswith("07 "), 'agriculture')
                        .when(col('func1').startswith('09 ') & ~col('func2').startswith("0368 ") & ~col('func2').startswith("0369 "), "transport" )
                        .when(col('func1').startswith('09 ') & (col('project1').startswith('24.0366.922 ') | col('project1').startswith('922 ')), 'road transport')
                        .when(col('func1').startswith('09 ') & col('func2').startswith('0367 '), 'air transport')
                        .when(col('func1').startswith('09 ') & col('func2').startswith('0369 '), 'telecom')
                        .when(col('func1').startswith('09 ') & col('func2').startswith('0368 '), 'energy')
                        .when(col('func1').startswith('08 ') & col('func2').startswith('0002 '), 'primary education')
                        .when(col('func1').startswith('08 ') & col('func2').startswith('0003 '), 'secondary education')
                        .when(col('func1').startswith('08 ') & 
                              (col('func2').startswith('0351 ') | col('func2').startswith('0347 ') | col('func2').startswith('0349 ') | col('func2').startswith('0353 ')), 'tertiary education')
                        )
            .withColumn('func', 
                        when(col("func1").startswith('06 '), 'Defence')
                        .when(col('func_sub').isin('judiciary', 'public safety'), 'Public order and safety')
                        .when( (col('year') <= 2017) & (col('func1').startswith('03 ') | col('func1').startswith("07 ")| col('func1').startswith("16 ")|col("func1").startswith("09 ")| col("func1").startswith("18 ")), "Economic affairs")
                        .when(((col("year") > 2017) & ((col('func1').startswith("07 "))| (col('func1').startswith("16 "))| (col("func1").startswith("09 "))| (col("func1").startswith("18 ")))), "Economic affairs")
                        .when(col('func1').startswith('10 '), 'Environmental protection')
                        .when(col('func1').startswith('17 '), 'Housing and community amenities')
                        .when(col('func1').startswith('13 '), "Health")
                        .when(col('func1').startswith('05 '), "Recreation, culture and religion")
                        .when(col('func1').startswith('08 '), 'Education')
                        .when(col('func1').startswith('11 '), 'Social protection')
                        .otherwise('General public services'))
            .withColumn('econ_sub',
                        when((col('exp_type') == "Personal") & ~col('econ2').startswith('06 ') & ~col('econ2').startswith('07 ') &  ~col('econ2').startswith('08 '), 'basic wages')
                        .when(col('exp_type') == "Personal", "allowances")
                        .when((col('exp_type')=="Inversion") & col('source_fin1').startswith('20 '), 'capital expenditure (foreign spending)')
                        .when(col('econ2').startswith('21 '), 'basic services')
                        .when(col('econ2').startswith('28 '), 'employment contracts')
                        .when(col('econ2').startswith('27 '), 'recurrent maintenance')
                        .when((col("year") < 2020) & ((col('econ2').startswith('52 ')) | (col('econ2').startswith('54')) | (col('econ2').startswith('04 ')) | (col('econ2').startswith('02 '))), 'subsidies to production')
                        .when((col("year") >= 2020) & ((col('econ2').startswith('04 ')) | (col('econ2').startswith('02 '))), 'subsidies to production')
                        .when(col('func1').startswith('11 ') & col('func2').startswith('0402') & col('econ1').startswith('5 '), 'pensions')
                        .when(col('func1').startswith('11 ') & col('econ1').startswith('5 '), 'social assistance'))
            .withColumn('econ',
                        when(col('econ1').startswith('6 '),'Interest on debt')
                        .when(col('exp_type') == 'Personal', 'Wage bill')
                        .when(col('exp_type') == 'Inversion','Capital expenditures')
                        .when(col('econ1').startswith('1 ') | col('econ1').startswith('2 ')| ((col('econ1').startswith('3 ')) &
                        (col('exp_type')=="Inversion")),'Goods and services')
                        .when(((col("year") < 2020) & ((col('econ2').startswith('52 ')) | (col('econ2').startswith('54')) | (col('econ2').startswith('04 ')) | (col('econ2').startswith('02 ')))), 'Subsidies')
                        .when(((col("year") >= 2020) & ((col('econ2').startswith('04 ')) | (col('econ2').startswith('02 ')))), 'Subsidies')
                        .when(col('econ_sub').isin('pensions', 'social assistance'), 'Social benefits')
                        .when((col('econ2').startswith('01 ') | col('econ2').startswith('51 ')) & ~col('func1').startswith('11 '), 'Grants and transfers')
                        .otherwise('Other expenses'))
            )

                        
@dlt.table(name=f'ury_boost_gold')
def boost_gold():
    return (dlt.read(f'ury_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                'executed',
                'is_foreign',
                'geo1',
                'admin0',
                'admin1',
                'admin2',
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

