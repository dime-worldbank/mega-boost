# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import (
    substring, col, lit, when, udf, trim, regexp_replace, initcap, concat, lower, create_map, coalesce
)

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
        'FUND': 'fund',
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
    # admin and geo data appear to be swapped
    df = df.withColumn(
        'admin0', when(col('county').startswith('00'), 'Central').otherwise('Regional')
    ).withColumn(
        'admin1', when(col('county').startswith('00'), 'Central Scope')
                 .otherwise(regexp_replace(col('county'), r'^\d+\s+', ''))
    ).withColumn(
        'admin2', regexp_replace(col('ministry'), r'^\d+\s+', '')
    )

    # --- Functional Classifications ---
    func_mapping = {
        "02": ["Defence",False],
        "03": ["Public order and safety",False],
        "04": ["Economic affairs",False],
        "05": ["Environmental protection",True],
        "06": ["Housing and community amenities",False],
        "07": ["Health",True],
        "08": ["Recreation, culture and religion",False],
        "09": ["Education",True],
        "10": ["Social protection",False]
    }

    func_filter = None
    for key, value_list in func_mapping.items():
        base_condition = (col('Func1').startswith(key))
        alt_condition_flag = value_list[1]
        condition_value = value_list[0]
        
        condition_1 = base_condition & not_dept 
        condition_2 = base_condition & col('Econ0').startswith('2')

        if alt_condition_flag == False:
            func_filter = func_filter.when(condition_1, condition_value) if not func_filter is None else when(condition_1, condition_value)
        else:
            func_filter = func_filter.when(condition_2, condition_value) if not func_filter is None else when(condition_2, condition_value)
        
    func_filter = func_filter.otherwise("General public services") 
    df = df.withColumn("func", func_filter)

    # --- Sub-Functional Classifications ---
    df = df.withColumn(
        # --- nested when
        'func_sub', when((col("func") == "Public order and safety"), when(col('Func2').startswith('033'), "judiciary").otherwise("public safety"))
            .when(not_dept & (col('Func2').startswith('042')), 'agriculture')
            .when(col('Func2').startswith('045'), 'transport')
            .when(not_dept & (col('Func3').startswith('0451')), 'roads')
            .when(col('ministry').startswith('429'), 'air transport')
            .when(not_dept & (col('Func2').startswith('043')), 'energy')
            .when(col('ministry').startswith('418'), 'telecoms')
            .when(col('Func2').startswith('07 ') | col('Func2').startswith('074'), 'primary and secondary health')
            .when(col('Func2').startswith('073'), 'tertiary and quaternary health')
    )
    
    # --- Econ and sub econ reused filters ---
    pensions_filter = (col('Econ0').startswith('2')) & (col('Econ2').startswith('271'))
    social_assistance_filter = ((col('Func1').startswith('10')) & col('Econ0').startswith('2')) # 2013 - ...
    allowances_filter = ((col('Econ2').startswith('211')) & not_dept)
    wage_filter = (col('Econ1').startswith('21'))

    # --- Sub-Economic Classifications ---     
    df = df.withColumn(
        'econ_sub', when(social_assistance_filter, 'social assistance')
            .when(wage_filter, when(allowances_filter, 'allowances').otherwise('basic wages'))
            .when(pensions_filter, 'pensions')
            .when(not_dept & (col('Econ2').startswith('212')), 'social benefits (pension contributions)')
            .when((col('Econ3').startswith('2213')) | (col('Econ3').startswith('2218')), 'basic services')
            .when(col('Econ3').startswith('2215'), 'recurrent maintenance')
    )

    # --- Economic Classifications ---     
    df = df.withColumn(
        'econ', when(wage_filter, 'Wage bill')
               .when(
                   (col('econ_sub') == 'social assistance') | (col('econ_sub') == 'pensions'), 'Social benefits')
               
               .when(
                   (col('budget').startswith('4')) 
                   & not_dept 
                   & (~col('Econ1').startswith('21'))
                   , 'Capital expenditures')
               .when(
                   (col('Econ1').startswith('22')) 
                   & (col('budget').startswith('1'))
                   , 'Goods and services')
               .when(
                   col('Econ1').startswith('25'), 
                   'Subsidies')
               
               .when(
                   (col('Econ1').startswith('24')) 
                   & not_dept
                   , 'Interest on debt')
               .when(
                    (col('Econ1').startswith('13') | col('Econ1').startswith('26')) & 
                    ~col('Func1').startswith('10') & 
                    col('budget').startswith('1') &  
                    not_dept,
                    'Other grants and transfers'
                )
               .otherwise('Other expenses')
    )

    # --- Foreign Classification ---
    df = df.withColumn(
        'is_foreign', (col('fund') == 'Foreign')
    )

    # --- Geo ---
    df = df.withColumn(
        'geo1', lower(col('admin1'))
    )

    return df

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
                'admin0',
                'admin1',
                'admin2',
                'econ_sub',
                'econ',
                'func_sub',
                'func',
                'is_foreign',
                'geo1'
                )
    )

