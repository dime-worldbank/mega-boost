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

    df = dlt.read('lbr_boost_bronze') 

    # Column renaming mapping
    rename_mappings = {
        'Econ0-Account_Class': 'Econ0',
        'Econ2-Sub_Item': 'Econ2',
        'Econ3-Sub_Sub_Item': 'Econ3',
        'Econ4-Sub_Sub_Sub_Item': 'Econ4',
        'Geo1-County': 'geo1',
        'Geo2-Disctrict': 'geo2',
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

    # Columns to coalesce (replace NULL with an empty string)
    columns_to_clean = [
        "Econ0", "Econ1", "Econ2", "Econ3", 
        "Econ4", "geo1", "FUND"
    ]
    for column in columns_to_clean:
        df = df.withColumn(column, coalesce(col(column).cast("string"), lit("")))

    # Filtering out unwanted values at the total expenditures level 
    df = df.filter((col('Econ0') != '4 Liabilities') & (col('Econ1') != '32 Financial assets'))

    # --- Admin and Geo Data Adjustments ---
    df = df.withColumn(
        'admin0', when(col('geo1').startswith('00'), 'Central').otherwise('Regional')
    ).withColumn(
        'admin1', when(col('geo1').startswith('00'), 'Central Scope')
                 .otherwise(regexp_replace(col('geo1'), r'^\d+\s+', ''))
    ).withColumn(
        'admin2', regexp_replace(col('ministry'), r'^\d+\s+', '')
    )

    # --- Sub-Functional Classifications ---
    df = df.withColumn(
        'func_sub', when((col("Func1").startswith('03')) & (col('Func3').startswith('0330')) & (~col('department').startswith('10401')), "judiciary")
                   .when((col("Func1").startswith('03')) & (~col('Func3').startswith('033')) & (~col('department').startswith('10401')), "public safety")
                   .when((~col('department').startswith('10401')) | (col('Func2').startswith('042')), 'agriculture')
                   .when(col('Func2').startswith('045'), 'transport')
                   .when((~col('department').startswith('10401')) | (col('Func3').startswith('0451')), 'roads')
                   .when(col('ministry').startswith('429'), 'air transport')
                   .when((~col('department').startswith('10401')) | (col('Func2').startswith('043')), 'energy')
                   .when(col('ministry').startswith('418'), 'telecoms')
                   .when(col('Func2').startswith('07 ') | col('Func2').startswith('074'), 'primary and secondary health')
                   .when(col('Func2').startswith('073'), 'tertiary and quaternary health')
    )

    # --- Functional Classifications ---
    df = df.withColumn(
        'func', when((col('Func1').startswith("01")) & (~col('department').startswith('10401')), "General public services")
               .when(col('Func1').startswith("02"), "Defense")
               .when(col("func_sub").isin("judiciary", "public safety"), "Public order and safety")
               .when(col('Func1').startswith("04"), "Economic affairs")
               .when(col('Func1').startswith("05"), "Environmental protection")
               .when(col('Func1').startswith("06"), "Housing and community amenities")
               .when(col('Func1').startswith("07"), "Health")
               .when(col('Func1').startswith("08"), "Recreation, culture and religion")
               .when(col('Func1').startswith("09"), "Education")
               .when(col('Func1').startswith("10"), "Social protection")
    )

    #  --- Sub-Economic Classifications ---     
    df = df.withColumn(
        'econ_sub', when((col('Econ1').startswith('21')) & (col('wages') == 'ALLOWANCES') & (~col('department').startswith('10401')), 'allowances')
                   .when((~col('department').startswith('10401')) & (col('Econ2').startswith('212')), 'social benefits (pension contributions)')
                   .when((col('budget').startswith('4')) & (~col('department').startswith('10401')) & (~col('Econ1').startswith('21')) & (col('FUND') == 'Foreign'), 'capital expenditure (foreign spending)')
                   .when((col('Econ3').startswith('2213')) | (col('Econ3').startswith('2218')), 'basic services')
                   .when(col('Econ3').startswith('2215'), 'recurrent maintenance')
                   .when((col('Func1').startswith('10')) & (~col('department').startswith('10401')), 'social assistance')
                   .when((col('Econ0').startswith('2')) & (col('Econ2').startswith('271')), 'pensions')
    )

    #  --- Economic Classifications ---     
    df = df.withColumn(
        'econ', when((col('Econ1').startswith('21')) & (~col('Econ0').startswith('4')) & (~col('Econ1').startswith('32')), 'Wage bill')
               .when((col('budget').startswith('4')) & (~col('department').startswith('10401')) & (~col('Econ1').startswith('21')), 'Capital expenditures')
               .when((col('Econ1').startswith('22')) & (col('budget').startswith('1')), 'Goods and services')
               .when(col('Econ1').startswith('25'), 'Subsidies')
               .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
               .when((col('Econ1').startswith('24')) & (~col('department').startswith('10401')), 'Interest on debt')
               .when(((col('Econ1').startswith('13')) | (col('Econ1').startswith('26'))) & (~col('Func1').startswith('10')) & (~col('Econ0').startswith('4')) & (~col('Econ1').startswith('32')) & (~col('department').startswith('10401')) & (col('budget').startswith('1')), 'other grants and transfers')
               .otherwise('Other expenses')
    )

    # --- Replacing misnomer column names ---
    df = df.withColumn(
        'admin2', regexp_replace(col('ministry'), r'^\d+\s+', '')
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

