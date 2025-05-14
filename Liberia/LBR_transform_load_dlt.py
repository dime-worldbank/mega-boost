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
    # window_spec = Window.orderBy(lit(1))
    # df = df.withColumn("index", row_number().over(window_spec))

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
        'econ1': 'Econ1',
        'econ2': 'Econ2',
        'econ3': 'Econ3',
        'econ4': 'Econ4',
        'geo1': 'Geo1',
        'geo2': 'Geo2',
        'admin1': 'Admin1',
        'admin2': 'Admin2',
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
        "econ0", "econ1", "econ2", "econ3", 
        "econ4", "geo1", "fund",'geo1','geo2','wages'
    ]

    for column in columns_to_clean:
        print(df.columns)
        df = df.withColumn(column, coalesce(col(column).cast("string"), lit("")))

    # --- Global Filters ---
    # Filtering out unwanted values at the total expenditures level
    df = df.filter((col('Econ0') != '4 Liabilities') & (col('Econ1') != '32 Financial assets'))

    # used quite often, so to limit repetition
    not_dept = ~col('Admin2').startswith('10401') 

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
        'func_sub', when((col("func") == "Public order and safety") & (col('Func2').startswith('033')), "judiciary")
            .when((col("func") == "Public order and safety") & (~col('Func2').startswith('033')), "public safety")
            .when(not_dept & (col('Func2').startswith('042')), 'agriculture')
            .when(col('Func2').startswith('045'), 'transport')
            .when(not_dept & (col('Func3').startswith('0451')), 'roads')
            .when(col('Admin1').startswith('429'), 'air transport')
            .when(not_dept & (col('Func2').startswith('043')), 'energy')
            .when(col('Admin1').startswith('418'), 'telecoms')
            .when(col('Func2').startswith('07 ') | col('Func2').startswith('074'), 'primary and secondary health')
            .when(col('Func2').startswith('073'), 'tertiary and quaternary health')
    )
    
    # --- Econ and sub econ reused filters ---
    pensions_filter = (col('Econ0').startswith('2')) & (col('Econ2').startswith('271'))
    social_assistance_filter = ((col('Func1').startswith('10')) & col('Econ0').startswith('2'))
    allowances_filter = ((col('Econ2').startswith('211')) & not_dept)
    wage_filter = (col('Econ1').startswith('21'))

    # --- Sub-Economic Classifications ---     
    df = df.withColumn(
        'econ_sub', when(social_assistance_filter, 'social assistance')
            .when(pensions_filter, 'pensions')
            .when(wage_filter & allowances_filter, 'allowances')
            .when(wage_filter & ~allowances_filter, 'basic wages')
            .when(not_dept & (col('Econ2').startswith('212')), 'social benefits (pension contributions)') 
            .when((col('Econ3').startswith('2213')) | (col('Econ3').startswith('2218')), 'basic services')
            .when(col('Econ3').startswith('2215'), 'recurrent maintenance')
    )

    # --- Economic Classifications ---
    df = df.withColumn(
        'econ', when(col('Econ1').startswith("21"), 
                    'Wage bill')
                
                .when(
                   col('Econ1').startswith('25'), 
                   'Subsidies')
                .when(
                   (col('Econ1').startswith('22')) 
                   & (col('budget').startswith('1'))
                   , 'Goods and services')
               .when(
                (col('year').cast('integer') < 2018) & col('Econ1').startswith('24'),
                'Interest on debt'
                )
                .when(
                (col('year').cast('integer') >= 2018) & col('Econ4').startswith('423104'),
                'Interest on debt'
                ).when(
                    (col('econ_sub') == 'social assistance') | (col('econ_sub') == 'pensions')
                    ,'Social benefits'
                )
                    # capex - admin2,"<>10401 Revenue",budget,"4*",Econ1,"<>21 Compensation of Employees"
                    # goods and services - ,Econ1,"22*",budget,"1 Recurrent"
                    # other grants - Econ1,{"13 grants","26 grants"},Func1,"<>10 Social Protection",Econ0,"<>4 Liabilities",Econ1,"<>32 Financial assets",budget,"1 Recurrent",admin2,"<>10401 Revenue"))
                    # wage bill - Econ1,"21 Compensation of Employees"
                .when(
                    (col('year').cast('integer') == 2018) & 
                    col('Econ1').startswith('26'),
                    'Other grants and transfers'
                ).when(
                    (col('Econ1').startswith('13') | col('Econ1').startswith('26')) & 
                    ~col('Func1').startswith('10') & 
                    col('budget').startswith('1') &  
                    not_dept,
                    'Other grants and transfers'
                )
                .when(
                   (col('budget').startswith('4')) 
                   & not_dept 
                   & (~col('Econ1').startswith('21'))
                   , 'Capital expenditures'
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
        "admin2",initcap(col("Admin2"))
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
        .filter(col('year') > 2008)
        .filter(col('year') < 2024)
        .filter(col('actual').isNotNull())
        .select(
                # 'index', 
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
