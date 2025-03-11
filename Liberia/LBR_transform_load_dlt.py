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
        .withColumn("Econ0", coalesce(col("Econ0").cast("string"), lit("")))
        .withColumn("Econ1", coalesce(col("Econ1").cast("string"), lit("")))
        .withColumn("Econ2-Sub_Item", coalesce(col("Econ2-Sub_Item").cast("string"), lit("")))
        .withColumn("Econ3-Sub_Sub_Item", coalesce(col("Econ3-Sub_Sub_Item").cast("string"), lit("")))
        .withColumn("Econ4-Sub_Sub_Sub_Item", coalesce(col("Econ4-Sub_Sub_Sub_Item").cast("string"), lit("")))
        .withColumn("Geo1-County", coalesce(col("Geo1-County").cast("string"), lit("")))
        .withColumn("FUND", coalesce(col("FUND").cast("string"), lit("")))
        .filter((col('Econ0') != '4 Liabilities') & (col('Econ1') != '32 Financial assets'))
        .withColumnRenamed('Econ2-Sub_Item', 'Econ2')
        .withColumnRenamed('Econ3-Sub_Sub_Item', 'Econ3')
        .withColumnRenamed('Econ4-Sub_Sub_Sub_Item', 'Econ4')
        .withColumnRenamed('Geo1-County', 'geo1')
        .withColumnRenamed('Geo2-Disctrict', 'geo2')
        .withColumnRenamed('Adm2-Department', 'admin2')
        .withColumnRenamed('Adm3-Section', 'admin3')
        .withColumnRenamed('Func1-Division', 'Func1')
        # Func2 is already named appropriately
        .withColumnRenamed('Func3-Functions', 'Func3')
        .withColumnRenamed('Bud_class', 'budget')
        .withColumnRenamed('WAGES', 'wages')
        .withColumnRenamed('F_Year', 'year')

        # In Liberia's case, admin and geo data appear to be swapped
        .withColumn('admin0',
            when(col('geo1').startswith('00'), 'Central')
            .otherwise('Regional')

        # admin1 - who spends the money
        ).withColumn('admin1',
            when(col('geo1').startswith('00'), 'Central Scope')
            .otherwise(regexp_replace(col('geo1'), r'^\d+\s+', ''))

        # No geo data
        
        # Functional classifications
        ).withColumn('func',
            when(((col('Func1').startswith("01")) & 
                  (~col('admin2').startswith('10401'))), "General public services")
            .when((col('Func1').startswith("02"), "Defense") & 
                  (col('Func1').startswith("02"), "Defense"))
            .when(col("func_sub").isin("judiciary", "public safety") , "Public order and safety")
            .when(col('Func1').startswith("04"), "Economic affairs") # Here named Economic relations
            .when(col('Func1').startswith("05"), "Environmental protection")
            .when(col('Func1').startswith("06"), "Housing and community amenities")
            .when(col('Func1').startswith("07"), "Health")
            .when(col('Func1').startswith("08"), "Recreation, culture and religion")
            .when(col('Func1').startswith("09"), "Education")
            .when(col('Func1').startswith("10"), "Social protection")

        # Sub Functional classifications 
        ).withColumn('func_sub',
            # judiciary
            when(((col("Func1").startswith('03')) & 
                (col('Func3').startswith('0330')) &
                (~col('admin2').startswith('10401')) ), "judiciary")
            # public safety
            .when(((col("Func1").startswith('03')) & 
                (~col('Func3').startswith('033')) &
                (~col('admin2').startswith('10401')) ), "public safety" ) # important for this to be after judiciary
            # agriculture
            .when((~(col('admin2').startswith('10401')) | 
                   (col('Func2').startswith('042'))), 'agriculture')
            # transport
            .when((col('Func2').startswith('045')), 'transport')
            # roads
            .when((~(col('admin2').startswith('10401')) | 
                   (col('Func3').startswith('0451'))), 'roads')
            # air transport
            .when((col('admin1').startswith('429')), 'air transport')
            # energy
            .when((~(col('admin2').startswith('10401')) | 
                   (col('Func2').startswith('043'))), 'energy')
            # telecoms
            .when((col('admin1').startswith('418')), 'telecoms')
            # health expenditure breakdown
            .when(((col('Func2').startswith('07 ')) | 
                   (col('Func2').startswith('074'))), 'primary and secondary health')
            .when(col('Func2').startswith('073'), 'tertiary and quaternary health')
            
            # education expenditure breakdown (primary, secondary, tertiary, quarterary) - None
        ).withColumn( 'econ_sub',
            # basic wages - No formula

            # allowances
            when(((col('Econ1').startswith('21')) &
                   (col('wages') == 'ALLOWANCES') &
                   (~col('admin2').startswith('10401'))), 'allowances')
            # pension contributions
            .when((~col('admin2').startswith('10401') &
                   (col('Econ2').startswith('212'))), 'social benefits (pension contributions)')
            # capital expenditure (foreign spending)
            .when(((col('budget').startswith('4')) &
                   (~col('admin2').startswith('10401')) &
                   (~col('Econ1').startswith('21')) &
                   (col('FUND') == 'Foreign')), 'capital expenditure (foreign spending)')
            # basic services
            .when(((col('Econ3').startswith('2213')) | 
                   (col('Econ3').startswith('2218'))), 'basic services')
            # recurrent maintenance
            .when((col('Econ3').startswith('2215')), 'recurrent maintenance')
            # social assistance
            .when(((col('Func1').startswith('10')) &
                   (~col('admin2').startswith('10401'))), 'social assistance')
            # pensions
            .when(((col('Econ0').startswith('2')) &
                   (col('Econ2').startswith('271'))), 'pensions')    
        ).withColumn('econ',
            # wage bill
            .when(((col('Econ1').startswith('21')) &
                   (~col('Econ0').startswith('4')) &
                   (~col('Econ1').startswith('32'))), 'Wage bill')     
            # capital expendiatures
            .when(((col('budget').startswith('4')) &
                   (~col('admin2').startswith('10401')) &
                   (~col('Econ1').startswith('21'))), 'Capital expenditures')
            # goods and services
            .when((col('Econ1').startswith('22') & 
                   col('budget').startswith('1')), 'Goods and services')
            # subsidies
            .when(col('Econ1').startswith('25'), 'Subsidies')
            # social benefits
            when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
            # interest on debt
            .when(col('Econ1').startswith('24') & 
                   (~col('admin2').startswith('10401')), 'Interest on debt')    
            # Other grants and transfers
            .when((((col('Econ1').startswith('13')) |
                   (col('Econ1').startswith('26'))) &
                   (~col('Func1').startswith('10')) &
                   (~col('Econ0').startswith('4')) &
                   (~col('Econ1').startswith('32')) &
                   (~col('admin2').startswith('10401')) &
                   (col('budget').startswith('1')), 'other grants and transfers')
            # other expenses
            .otherwise('Other expenses')
            )
        )
    )

@dlt.table(name=f'lbr_boost_gold')
def boost_gold():
    # adming and geo data appear to be swapped
    # there is no true geo data

    return (dlt.read(f'lbr_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .filter(col('year') > 2008)
        .select('country_name',
                col('year').cast("integer"),
                col('Original_appr').alias('approved'),
                col('Actual').alias('executed'),
                col('Revised_appr').alias('revised'),
                col('geo1').alias('admin1'),
                'admin0',
                'admin1',
                'admin2',
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

