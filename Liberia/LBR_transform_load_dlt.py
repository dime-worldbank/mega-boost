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
    return (dlt.read('lbr_boost_bronze')
        .withColumn("Econ0-Account_Class", coalesce(col("Econ0-Account_Class").cast("string"), lit("")))
        .withColumn("Econ1", coalesce(col("Econ1").cast("string"), lit("")))
        .withColumn("Econ2-Sub_Item", coalesce(col("Econ2-Sub_Item").cast("string"), lit("")))
        .withColumn("Econ3-Sub_Sub_Item", coalesce(col("Econ3-Sub_Sub_Item").cast("string"), lit("")))
        .withColumn("Econ4-Sub_Sub_Sub_Item", coalesce(col("Econ4-Sub_Sub_Sub_Item").cast("string"), lit("")))
        .withColumn("Geo1-County", coalesce(col("Geo1-County").cast("string"), lit("")))
        .withColumn("FUND", coalesce(col("FUND").cast("string"), lit("")))
        .filter((col('Econ0-Account_Class') == '4 Liabilities') & (lower(col('Econ1')) != '32 Financial assets'))
        .withColumnRenamed('Econ0-Account_Class', 'econ0')
        .withColumnRenamed('Econ1', 'Econ1')
        .withColumnRenamed('Econ2-Sub_Item', 'Econ2')
        .withColumnRenamed('Econ3-Sub_Sub_Item', 'Econ3')
        .withColumnRenamed('Econ4-Sub_Sub_Sub_Item', 'Econ4')
        .withColumnRenamed('Geo1-County', 'geo1')
        .withColumnRenamed('Adm1-Ministry', 'admin1')
        .withColumnRenamed('Adm2-Department', 'admin2')
        .withColumnRenamed('Func1-Division', 'Func1')
        .withColumnRenamed('Func3-Functions', 'Func3')
        .withColumnRenamed('Bud_class', 'budget')
        .withColumnRenamed('WAGES', 'wages')
        .withColumnRenamed('F_Year', 'year')
        .withColumn('admin0',
            when(col('geo1').startswith('00'), 'Central')
            .otherwise('Regional')
        ).withColumn('geo1',
            when(col('geo1').startswith('00'), 'Central Scope')
        ).withColumn('func_sub',
            # judiciary breakdown
            when(((col("Func1").startswith('03')) & 
                (col('Func3').startswith('0330'))), "allowances in judiciary")
            # public safety
            .when(col("Func1").startswith('03'), "public safety" ) # important for this to be after judiciary
            # education expenditure breakdown
            .when(((col('Func1').startswith('09')) &
                (col('Func2').startswith('091'))), 'primary education')
            .when(((col('Func1').startswith('09')) &
                (col('Func2').startswith('092'))), 'secondary education')
            .when(((col('Func1').startswith('09')) & 
                (col('Func2').startswith('094'))), 'tertiary education')
            # health expenditure breakdown
            .when(col('Func2').startswith('07411'), 'primary and secondary health')
            .when(((col('Func2').startswith('07311')) | 
                   (col('Func2').startswith('07321'))), 'tertiary and quaternary health')
        ).withColumn('func',
            when(col('Func1').startswith("01"), "General public services")
            .when(col('Func1').startswith("02"), "Defence")
            .when(col("func_sub").isin("judiciary", "public safety") , "Public order and safety")
            .when(col('Func1').startswith("04"), "Economic affairs")
            .when(col('Func1').startswith("05"), "Environmental protection")
            .when(col('Func1').startswith("06"), "Housing and community amenities")
            .when(col('Func1').startswith("07"), "Health")
            .when(col('Func1').startswith("08"), "Recreation, culture and religion")
            .when(col('Func1').startswith("09"), "Education")
            .when(col('Func1').startswith("10"), "Social protection")
        ).withColumn( 'econ_sub',
            # allowances
            when(((col('Econ1').startswith('21')) &
                   (col('wages') == 'ALLOWANCES')), 'allowances')
            # pensions
            .when(((col('Econ2').startswith('212')) &
                   (~col('admin2').startswith('10401'))), 'pensions')
            # capital expenditure (foreign funded)
            .when(((col('budget').startswith('4')) &
                   (~col('admin2').startswith('10401')) &
                   (~col('Econ1').startswith('21')) &
                   (col('FUND') != 'Foreign')), 'capital expenditure (foreign spending)')
            # recurrent maintenance
            .when((col('Econ3').startswith('2215')), 'recurrent maintenance')

            # social assistance
            .when(((col('Func1').startswith('10')) &
                   (~col('admin2').startswith('10401'))), 'capital expenditure (foreign spending)')
            # basic wages - No formula
            
            # basic services
            .when(((col('Econ3').startswith('2213')) | 
                   (col('Econ3').startswith('2218'))), 'basic services')
            # employment contracts - None
            
            # subsidies to production - None
            .when(col('Econ1').startswith('25'), 'subsidies to production') # same as the value for subsidies

        ).withColumn('econ',
            # social benefits
            when(((col('Econ1').startswith('21')) &
                   (~col('Econ0').startswith('4')) &
                  (~col('Econ1').startswith('32'))), 'Social benefits') # should come before other econ categories
            # capital expendiatures
            .when(((col('budget').startswith('4')) &
                   (~col('admin2').startswith('10401')) &
                   (~col('Econ1').startswith('21'))), 'Capital expenditures')
            # wage bill
            .when(((col('Econ1').startswith('21')) &
                   (~col('Econ0').startswith('4')) &
                   (~col('Econ1').startswith('32'))), 'Wage bill')     
            # goods and services
            .when((col('Econ1').startswith('22') & 
                   col('budget').startswith('1')), 'Goods and services')
            # subsidies
            .when(col('Econ1').startswith('24') & 
                   (col('budget').startswith('1')), 'Subsidies')
            # interest on debt
            .when(col('Econ1').startswith('24') & 
                   (~col('admin2').startswith('10401')), 'Interest on debt')           
            # other expenses
            .otherwise('Other expenses')
            # other grants?
        )
    )

@dlt.table(name=f'lbr_boost_gold')
def boost_gold():
    return (dlt.read(f'lbr_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .filter(col('year') > 2008)
        .select('country_name',
                col('year').cast("integer"),
                col('Original_appr').alias('approved'),
                col('Actual').alias('executed'),
                col('Revised_appr').alias('revised'),
                'geo1',
                'admin0',
                'admin1',
                'admin2',
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

