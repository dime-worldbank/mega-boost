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

@dlt.table(name='lbr_boost_bronze')
def boost_bronze():
    return (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/Data.csv'))

@dlt.table(name='lbr_boost_silver')
def boost_silver():

    df = dlt.read('lbr_boost_bronze') 

    # --- Column renaming mapping ---
    rename_mappings = {
        'Econ0-Account_Class': 'Econ0',
        'Econ2-Sub_Item': 'Econ2',
        'Econ3-Sub_Sub_Item': 'Econ3',
        'Econ4-Sub_Sub_Sub_Item': 'Econ4',
        'Geo1-County': 'county',
        'Adm1-Ministry': 'ministry',
        'Adm2-Department': 'department',
        'Func1-Division': 'Func1',
        'Func3-Functions': 'Func3',
        'Bud_class': 'budget',
        'WAGES': 'wages',
        'F_Year': 'year'
    }

    for old_name, new_name in rename_mappings.items():
        df = df.withColumnRenamed(old_name, new_name)

    columns_to_clean = [
        "Econ0", "Econ1", "Econ2", "Econ3", 
        "Econ4", "county", "FUND"
    ]
    for column in columns_to_clean:
        df = df.withColumn(column, coalesce(col(column).cast("string"), lit("")))

    # --- Global Filters ---
    # Filtering out unwanted values at the total expenditures level
    df = df.filter((col('Econ0') != '4 Liabilities') & (col('Econ1') != '32 Financial assets'))

    # used quite often, so to limit repetition
    not_dept = ~col('department').startswith('10401') 

    # --- Admin and Geo Data Adjustments ---
    df = df.withColumn(
        'admin0', when(col('county').startswith('00'), 'Central').otherwise('Regional')
    ).withColumn(
        'admin1', when(col('county').startswith('00'), 'Central Scope')
                 .otherwise(regexp_replace(col('county'), r'^\d+\s+', ''))
    ).withColumn(
        'admin2', regexp_replace(col('ministry'), r'^\d+\s+', '')
    )

    # --- Sub-Functional Classifications ---
    df = df.withColumn(
        'func_sub', when((col("Func1").startswith('03')) & (col('Func3').startswith('0330')) & not_dept, "judiciary")
            .when((col("Func1").startswith('03')) & (~col('Func3').startswith('033')) & not_dept, "public safety")
            .when(not_dept & (col('Func2').startswith('042')), 'agriculture')
            .when(col('Func2').startswith('045'), 'transport')
            .when(not_dept & (col('Func3').startswith('0451')), 'roads')
            .when(col('ministry').startswith('429'), 'air transport')
            .when(not_dept & (col('Func2').startswith('043')), 'energy')
            .when(col('ministry').startswith('418'), 'telecoms')
            .when(col('Func2').startswith('07 ') & col('Func2').startswith('074'), 'primary and secondary health')
            .when(col('Func2').startswith('073'), 'tertiary and quaternary health')
    )

    # --- Functional Classifications ---
    func_mapping = {
        "02": "Defense",
        "03": "Public order and safety",
        "04": "Economic affairs",
        "05": "Environmental protection",
        "06": "Housing and community amenities",
        "07": "Health",
        "08": "Recreation, culture and religion",
        "09": "Education",
        "10": "Social protection"
    }

    func_col = None
    for key, value in func_mapping.items():
        # this is the general condition of each function mapping
        condition = (col('Func1').startswith(key)) & not_dept 

        func_col = func_col.when(condition & not_dept, value) if not func_col is None else when(condition, value)

    func_col = func_col.otherwise("General public services") 
    df = df.withColumn("func", func_col)

    # --- Temporary columns to calculate 'basic wages' ---
    df = df.withColumn(
        'wage_bill', when((col('Econ1').startswith('21')) & (~col('Econ0').startswith('4')), True).otherwise(False)
    )

    df = df.withColumn(
        'allowances', when((col('Econ1').startswith('21')) & (col('wages') == 'ALLOWANCES') & not_dept, True).otherwise(False)
    )

    # --- Sub-Economic Classifications ---     
    df = df.withColumn(
        ## How to add Spending in Basic Wages
        'econ_sub', when(col('wage_bill') & ~col('allowances'), 'basic wages')
            .when((col('Econ1').startswith('21')) & (col('wages') == 'ALLOWANCES') & not_dept, 'allowances')
            .when(not_dept & (col('Econ2').startswith('212')), 'social benefits (pension contributions)')
            .when((col('budget').startswith('4')) & not_dept & (~col('Econ1').startswith('21')) & (col('FUND') == 'Foreign'), 'capital expenditure (foreign spending)')
            .when((col('Econ3').startswith('2213')) | (col('Econ3').startswith('2218')), 'basic services')
            .when(col('Econ3').startswith('2215'), 'recurrent maintenance')
            .when((col('Func1').startswith('10')) & not_dept, 'social assistance')
            .when((col('Econ0').startswith('2')) & (col('Econ2').startswith('271')), 'pensions')
    )

    # --- Economic Classifications ---     
    df = df.withColumn(
        'econ', when((col('Econ1').startswith('21')) & (~col('Econ0').startswith('4')), 'Wage bill') # removed global condition
               .when((col('budget').startswith('4')) & not_dept & (~col('Econ1').startswith('21')), 'Capital expenditures')
               .when((col('Econ1').startswith('22')) & (col('budget').startswith('1')), 'Goods and services')
               .when(col('Econ1').startswith('25'), 'Subsidies')
               .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
               .when((col('Econ1').startswith('24')) & not_dept, 'Interest on debt')
               .when(((col('Econ1').startswith('13')) | (col('Econ1').startswith('26'))) & (~col('Func1').startswith('10')) & (~col('Econ0').startswith('4')) & not_dept & (col('budget').startswith('1')), 'other grants and transfers') # removed global condition
               .otherwise('Other expenses')
    )

    return df

@dlt.table(name=f'lbr_boost_gold')
def boost_gold():
    # admin and geo data appear to be swapped
    # there is no geo data

    return (dlt.read(f'lbr_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .filter(col('year') > 2008)
        .select('country_name',
                col('year').cast("integer"),
                col('Original_appr').alias('approved'),
                col('Actual').alias('executed'),
                col('Revised_appr').alias('revised'),
                'admin0',
                'admin1',
                'admin2',
                'econ_sub',
                'econ',
                'func_sub',
                'func'
                )
    )

