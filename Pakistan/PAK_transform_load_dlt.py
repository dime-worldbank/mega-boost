# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, expr
from pyspark.sql.types import StringType


TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
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
        .filter(~col('econ1').startswith('A08 ') & ~col('econ1').startswith('A10 ') & ~col('econ1').startswith('A99 '))
        .withColumn(
            'admin0_tmp', 
            when(col('Admin0')=='Federal', 'Central')
            .otherwise('Regional')
        ).withColumn(
            'admin1_tmp', # since admin1 already exists in the raw data
            when(col('admin0_tmp')=='Central', 'Central Scope')
            .when(col("Admin0") == "KP", 'Khyber Pakhtunkhwa')
            .otherwise(col('Admin0'))
        ).withColumn(
            'admin2_tmp',
            initcap(trim(expr("substring(admin1, instr(admin1, '-') + 1)")))
        ).withColumn(
            'geo1', 
            when(col('admin0_tmp')=='Central', 'Central Scope') # since we don't have geo tagged spending information
            .otherwise(col('admin1_tmp'))
        ).withColumn(
            'func_sub',
            when(col('func2').startswith('031'), 'Judiciary')
            .when((col('func1').startswith('03') & (~col('func2').startswith('031'))), 'Public Safety')
            .when(col('func2').startswith('091'), 'Primary Education')
            .when(col('func2').startswith('092'), 'Secondary Education')
            .when(col('func2').startswith('093'), 'Tertiary Education')
        ).withColumn(
            'func',
            when(col('func1').startswith('02'), 'Defence')
            .when(col('func1').startswith('03'), 'Public order and safety')
            .when(col('func1').startswith('04'), 'Economic affairs')
            .when(col('func1').startswith('05'), 'Environmental protection')
            .when(col('func1').startswith('06'), 'Housing and community amenities')
            .when(col('func1').startswith('07'), 'Health')
            .when(col('func1').startswith('08'), 'Recreation, culture and religion')
            .when(col('func1').startswith('09'), 'Education')
            .when(col('func1').startswith('10'), 'Social protection')
            .otherwise(lit('General public services'))
        ).withColumn(
            'econ_sub',
            when(col('econ2').startswith('A011'), 'Basic Wages')
            .when(col('econ2').startswith('A012'), 'Allowances')
            .when(((col('econ2').startswith('A033')) | (col('econ2').startswith('A034'))), 'Basic Services')
            .when(col('econ1').startswith('A13'), 'Recurrent Maintenance')
            .when(col('econ2').startswith('A051'), 'Subsidies to Production')
            .when((col('func1').startswith('10') & col('econ1').startswith('A05')), 'Social Assistance')
            .when(col('econ1').startswith('A04'), 'Pensions')
        ).withColumn(
            'econ',
            # subsidies
            when(col('econ2').startswith('A051'), 'Subsidies')
            # wage bill
            .when(col('econ1').startswith('A01'), 'Wage bill')
            # cap ex
            .when(((col('capital')=='y') & (~col('econ1').startswith('A01')) & (~col('econ2').startswith('A051'))), 'Capital expenditures') 
            # goods and services
            .when(((col('econ1').startswith('A03')) & (~col('econ2').startswith('A051')) & ((col('capital') != 'y') | col('capital').isNull())), 'Goods and services')
            # social benefits
            .when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits')
            # other grants and transfers
            .when((col('econ1').startswith('A05')) & (~col('func1').startswith('10')) & (~col('econ2').startswith('A051')), 'Other grants and transfers')
            # interest on debt
            .when(col('econ1').startswith('A07 '), 'Interest on debt') 
            .otherwise('Other expenses') 
        ).withColumn('is_transfer', lit(False))
    )

@dlt.table(name=f'pak_boost_gold')
def boost_gold():
    return (dlt.read(f'pak_boost_silver')
            .withColumn('country_name', lit(COUNTRY))
            .select('country_name',
                    'year',
                    'approved',
                    expr("CAST(NULL AS DOUBLE) as revised"),
                    'executed',
                    col('admin0_tmp').alias('admin0'),
                    col('admin1_tmp').alias('admin1'),
                    col('admin2_tmp').alias('admin2'),
                    'geo1',
                    'is_transfer',
                    'func',
                    'func_sub',
                    'econ',
                    'econ_sub'
        )
    )
