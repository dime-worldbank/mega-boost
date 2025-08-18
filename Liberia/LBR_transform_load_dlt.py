# Databricks notebook source
# DBTITLE 1,Main
import dlt
import unicodedata
from pyspark.sql.functions import (
    substring, col, lit, when, udf, trim, regexp_replace, initcap, concat, lower, create_map, coalesce, split, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
    df = (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/Data.csv'))

    # Add index column
    window_spec = Window.orderBy(lit(1))
    df = df.withColumn("index", row_number().over(window_spec))

    return df

@dlt.table(name='lbr_boost_silver')
def boost_silver():
    df = dlt.read('lbr_boost_bronze') 

    # --- Dropping unwanted columns ---
    bad_cols = ['year']

    for bad_col in bad_cols:
        if bad_col in df.columns:
            df = df.drop(bad_col)

    # --- Column renaming mapping ---
    rename_mappings = {
        'year.1': 'year',
        'econ0': 'Econ0',
        'econ1': 'Econ1_orig',
        'econ2': 'Econ2',
        'econ3': 'Econ3',
        'econ4': 'Econ4',
        'geo1': 'Geo1',
        'geo2': 'Geo2',
        'admin1': 'Admin1',
        'admin2': 'Admin2_orig',
        'admin3': 'Admin3',
        'func1': 'Func1',
        'func214': 'Func3',
        'Func226': 'Func2',
        'budget_class': 'budget',
        'WAGES': 'wages',
        'FUND25': 'fund',
    }

    for old_name, new_name in rename_mappings.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    columns_to_clean = [
        "econ0", "Econ1_orig", "econ2", "econ3", 
        "econ4", "geo1", "fund",'geo1','geo2','wages'
    ]

    for column in columns_to_clean:
        print(df.columns)
        df = df.withColumn(column, coalesce(col(column).cast("string"), lit("")))

    # --- Global Filters ---
    # Filtering out unwanted values at the total expenditures level
    df = df.filter((col('Econ0') != '4 Liabilities') & (col('Econ1_orig') != '32 Financial assets'))

    # used quite often, so to limit repetition
    not_dept = ~col('Admin2_orig').startswith('10401 Revenue') 

    # --- Admin and Geo Data Adjustments ---
    # admin and geo data appear to be swapped
    df = df.withColumn(
        'admin0', when(col('Geo1').startswith('00'), 'Central').otherwise('Regional')
    ).withColumn(
        'admin1', when(col('Geo1').startswith('00'), 'Central Scope')
                 .otherwise(regexp_replace(col('Geo1'), r'^\d+\s+', ''))
    ).withColumn(
        'admin2', regexp_replace(col('Admin1'), r'^\d+\s+', '')
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
        'func_sub', when((col("func") == "Public order and safety") & (col('Func2').startswith('033')), "Judiciary")
            .when((col("func") == "Public order and safety") & (~col('Func2').startswith('033')), "Public Safety")
            .when(not_dept & (col('Func2').startswith('042')), 'Agriculture')
            .when(col('Func2').startswith('045'), 'Transport')
            .when(not_dept & (col('Func3').startswith('0451')), 'Roads')
            .when(col('Admin1').startswith('429'), 'Air Transport')
            .when(not_dept & (col('Func2').startswith('043')), 'Energy')
            .when(col('Admin1').startswith('418'), 'Telecoms')
            .when(col('Func2').startswith('07 ') | col('Func2').startswith('074'), 'Primary and Secondary Health')
            .when(col('Func2').startswith('073'), 'Tertiary and Quaternary Health')
            # .otherwise('Unknown')
    )
    # df = df.withColumn("func_sub", initcap("func_sub"))
    
    # --- Econ and sub econ reused filters ---
    pensions_filter = (col('Econ0').startswith('2')) & (col('Econ2').startswith('271'))
    social_assistance_filter = ((col('Func1').startswith('10')) & col('Econ0').startswith('2'))
    allowances_filter = ((col('Econ2').startswith('211')) & not_dept)
    wage_filter = (col('Econ1_orig').startswith('21'))

    # --- Sub-Economic Classifications ---     
    df = df.withColumn(
        'econ_sub', when(social_assistance_filter, 'Social Assistance')
            .when(pensions_filter, 'Pensions')
            .when(wage_filter & allowances_filter, 'Allowances')
            .when(wage_filter & ~allowances_filter, 'Basic Wages')
            .when(not_dept & (col('Econ2').startswith('212')), 'Social Benefits (Pension Contributions)') 
            .when((col('Econ3').startswith('2213')) | (col('Econ3').startswith('2218')), 'Basic Services')
            .when(col('Econ3').startswith('2215'), 'Recurrent Maintenance')
            # .otherwise('Unknown')
    )
    df = df.withColumn("econ_sub", initcap("econ_sub"))

    # --- Economic Classifications ---
    subsidy_filter = col('Econ1_orig').startswith('25')
    social_benefits_filter= ((col('econ_sub') == 'social assistance') | (col('econ_sub') == 'pensions'))
    df = df.withColumn(
        'econ', 
                when(col('Econ1_orig').startswith("21")
                     & ~social_assistance_filter
                     , 'Wage bill'
                )
                .when(
                    (col('budget') == '4 Public Investment (PSIP)') 
                    & not_dept 
                    & ~(col('Econ1_orig') == '21 Compensation of Employees')
                    & ~social_assistance_filter
                    , 'Capital expenditures'
                )
                .when(
                    subsidy_filter
                    & ~social_assistance_filter
                    , 'Subsidies'
                )
                .when(
                    (col('Econ1_orig').startswith('22')) 
                    & (col('budget').startswith('1'))
                    & ~social_assistance_filter
                    , 'Goods and services'
                )
                .when(
                    (col('year').cast('integer') < 2018) & col('Econ1_orig').startswith('24')
                    & ~social_assistance_filter
                    , 'Interest on debt'
                )
                .when(
                    (col('year').cast('integer') >= 2018) & col('Econ4').startswith('423104')
                    & ~social_assistance_filter
                    , 'Interest on debt'
                )
                .when(
                    social_benefits_filter
                    , 'Social benefits'
                )
                .when(
                    (col('year').cast('integer') == 2018) 
                    & col('Econ1_orig').startswith('26')
                    , 'Other grants and transfers'
                )
                .when(
                    (col('Econ1_orig').startswith('13') | col('Econ1_orig').startswith('26')) 
                    & ~col('Func1').startswith('1')
                    & col('budget').startswith('1')
                    & not_dept
                    , 'Other grants and transfers'
                )
               .otherwise('Other expenses')
    )

    # --- Foreign Classification ---
    df = df.withColumn(
        'is_foreign', (col('fund') == 'Foreign')
    )

    # --- Geo ---
    df = df.withColumn(
        'geo1', initcap(col('admin1'))
    )
    # --- Cases ---
    df = df.withColumn(
        "admin1",initcap(col("Admin1"))
    )

    df = df.withColumn(
        "admin2",initcap(col("Admin2_orig"))
    )

    df = df.withColumn(
        "admin1", when(col("admin1").isin("Bong County"), "Bong")
        .when(col("admin1").isin("Bomi County"), "Bomi")
        .when(col("admin1") == "Rivergee", "River Gee")
        .when(col("admin1") == "Rivercess", "River Cess")
        .otherwise(col("admin1"))
    )

    return df

@dlt.table(name=f'lbr_boost_gold')
def boost_gold():
    return (dlt.read(f'lbr_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        # .filter(col('actual').isNotNull())
        # .filter(col('year') < 2025)
        .select(
                # 'index', 
                #     'Admin2_orig',
                #     "Econ1_orig",
                #     'budget',
                'country_name',
                col('year').cast('integer'),
                col('approved'),
                col('actual').alias('executed'),
                col('revised'),
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
