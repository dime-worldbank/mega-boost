# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf, trim, regexp_replace, initcap, concat
from pyspark.sql.types import StringType

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Albania'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'alb_boost_bronze')
def boost_bronze_cen():
    # Load the data from CSV
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Data_Expenditures.csv')
    )

@dlt.table(name=f'alb_boost_silver')
def boost_silver():
    return (dlt.read(f'alb_boost_bronze')          
            .filter(col('transfer') == 'Excluding transfers'
            ).withColumn('is_foreign', col('fin_source').startswith('2')
            ).withColumn('admin0',
                        when(col('admin1')=='Local', 'Regional')
                        .otherwise('Central')                
            ).withColumn('admin2_tmp',
                        when(col('counties')=='Central', col('admin3'))
                        .otherwise(col('counties'))
            ).withColumn('geo1',
                        when(col('counties')=='Central', 'Central Scope')
                        .otherwise(col('counties'))
            ).withColumn('func_sub',
                        # spending in judiciary
                        when(col('func2').startswith('033'), 'judiciary')
                        # public safety
                        .when(col('func2').substr(1,3).isin(['031', '034', '035']), 'public safety')
                        # spending in energy
                        .when(col('func2').startswith('043'), 'Energy')
                        # primary and secondary health
                        .when(col('func2').startswith('072') | col('func2').startswith('074'), 'primary and secondary health')
                        # tertitaey and quaternary health
                        .when(col('func2').startswith('073'), 'tertiary and quaternary health')
                        # primary education
                        .when(col('func1').startswith('09') & col('func2').startswith('091'), 'primary education')
                        # secondary education
                        .when(col('func1').startswith('09') & col('func2').startswith('092'), 'secondary education')
                        # tertiary education
                        .when(col('func1').startswith('09') & col('func2').startswith('094'), 'tertiary education')
            ).withColumn('func',
                        # public order and safety
                        when(col('func_sub').isin('judiciary', 'public safety'), 'Public order and safety')
                        # defense
                        .when(col('func1').startswith('02'), 'Defence')
                        # economic relations
                        .when(col('func1').startswith('04'), 'Economic affairs')
                        # environmental protection
                        .when(col('func1').startswith('05'), 'Environmental protection')
                        # housing
                        .when(col('func1').startswith('06'), 'Housing and community amenities')
                        # health
                        .when(col('func1').startswith('07'), 'Health')
                        # recreation, culture, religion
                        .when(col('func1').startswith('08'), 'Recreation, culture and religion')
                        # education
                        .when(col('func1').startswith('09'), 'Education')
                        # social protection 
                        .when(col('func1').startswith('10'), 'Social protection')
                        # general public services
                        .otherwise('General public services')
            ).withColumn('econ_sub',
                        # allowances
                        when((col('econ3').startswith('600') & 
                               col('econ5').substr(1, 7).isin(['6001005', '6001003', '6001006', '6001009', '6001099', '6001008', '6001014', '6001007', '6001012', '6001004'])), 'allowances')
                        # basic wages
                        .when(col('econ3').startswith('600') | col('econ3').startswith('601'), 'bsaic wages')
                        # pension contributions
                        .when(col('econ3').startswith('601'), 'social benefits (pension contributions)') # note this will be zero since it is subsumed into above category
                        # capital expenditures (foreign funded)
                        .when(col('is_foreign') & col('exp_type').startswith('3'), 'capital expenditure (foreign funded)')
                        # no entry for capital maintenance
                        # goods and services (basic services)
                        .when(col('econ4').startswith('6022') | col('econ4').startswith('6026'), 'basic services')
                        # no entry for employment contracts
                        # recurrent maintenance
                        .when(col('econ4').startswith('6025'), 'recurrent maintenance')
                        # subsidies to production
                        .when(col('econ3').startswith('603'), 'subsidies to production')
                        # social assistance
                        .when(col('econ3').startswith('606') & col('func2').startswith('104'), 'social assistance')
                        # pensions
                        .when(col('econ3').startswith('606') & col('func2').startswith('102'), 'pensions')
                        # other social benefits
                        .when(col('econ3').startswith('606') & col('func2').startswith('10'), 'other social benefits') # should come after social assistance and pensions
            ).withColumn('econ',         
                        # wage bill
                        when(col('econ3').startswith('601') | col('econ3').startswith('600'), 'Wage bill')
                        # capital expenditure
                        .when(col('exp_type').startswith('3'), 'Capital expenditures')
                        # goods and services
                        .when(col('econ3').startswith('602'), 'Goods and services')
                        # subsidies
                        .when(col('econ3').startswith('603'), 'Subsidies')
                        # social benefits
                        .when(col('econ3').startswith('606') & col('func2').startswith('10'), 'Social benefits')
                        # other grants and transfers
                        .when(col('econ3').startswith('604') | col('econ3').startswith('605'), 'Other grants and transfers')
                        # interest on debt
                        .when(col('econ2').startswith('65') | col('econ2').startswith('66'), 'Interest on debt')
                        # other expenses
                        .otherwise('Other expenses')
                        )
            )
    
@dlt.table(name=f'alb_boost_gold')
def boost_gold():
    return (dlt.read(f'alb_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                'revised',
                'executed',
                'is_foreign',
                'geo1',
                'admin0',
                col('counties').alias('admin1'),
                col('admin2_tmp').alias('admin2'),
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )
           
