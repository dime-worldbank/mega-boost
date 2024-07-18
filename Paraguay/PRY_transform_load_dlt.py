# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf, trim, regexp_replace, initcap, concat
from pyspark.sql.types import StringType

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Paraguay'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'pry_boost_bronze_cen')
def boost_bronze_cen():
    # Load the data from CSV
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/cen.csv')
            .filter(~col('ECON4').startswith('600') &
                    ~col('ECON5').startswith('730') &
                    ~col('ECON5').startswith('740') &
                    (~col('TRANS').isNotNull())
            ).withColumn('sheet', lit('cen'))
            .withColumn('adm1_name_tmp', initcap(trim(regexp_replace(col("GEO1"), "^.+-", ""))) 
            ).withColumn('adm1_name',
                when(col("adm1_name_tmp") == 'Alcance Nacional', 'Central Scope')
                .when(col("adm1_name_tmp") == 'Auxiliar Traspaso', 'Auxiliary Transfer')
                .when(col("adm1_name_tmp") == 'No Disponible', 'Other')
                .otherwise(col("adm1_name_tmp"))
            ).withColumn(
                'is_transfer', col('TRANS').isNotNull()                        
            ).withColumn(
                'is_foreign', (~col('is_transfer')) & (col('Foreign')=='Foreign')
            ).withColumn(
                'admin0', lit('Central')
            ).withColumn(
                'admin1', lit('Central')
            ).withColumn(
                'admin2', initcap(trim(regexp_replace('ADMIN1', "^[\d\s-]+", '')))
            ).withColumn('geo1_tmp',
                when(col("adm1_name_tmp") == 'Alcance Nacional', 'Central Scope')
                .when(col("adm1_name_tmp") == 'Auxiliar Traspaso', 'Auxiliary Transfer')
                .when(col("adm1_name_tmp") == 'No Disponible', 'Other')
                .otherwise(col("adm1_name_tmp"))
            ).drop(
                'adm1_name_tmp'
            ).withColumn('func_sub',
                when(col('FUNCTION2').startswith('120'), "judiciary")
                .when(col('FUNCTION2').startswith('220'), "public safety")
                .when(((col('PROGRAM1').startswith('001') | col('PROGRAM1').startswith('007')) & (col('hospital').isNull())), "primary and secondary health")
                .when((col('ECON2').startswith('120') & col('ECON5').startswith('240')), "tertiaty and quaternary health")
                .when(col('FUNCTION3').startswith('341'), "primary education")
                .when(col('FUNCTION3').startswith('342'), "secondary education") # No further information about higher education 
            ).withColumn('func', 
                when(col("func_sub").isin("judiciary", "public safety"), "Public order and safety")
                .when(col('FUNCTION2').startswith('210'), 'Defence')
                .when((col('FUNCTION1').startswith('400') | col('FUNCTION1').startswith('600')), 'Economic affairs')
                .when(col('FUNCTION2').startswith('310'), 'Health')
                .when((col('FUNCTION3').startswith('344') | col('FUNCTION3').startswith('345')), 'Recreation, culture and religion')
                .when(col('FUNCTION2').startswith('340'), 'Education')
                .when(col('FUNCTION2').startswith('440'), 'Environmental protection')
                .when((col('FUNCTION2').startswith('370') | col('FUNCTION2').startswith('380') | col('FUNCTION2').startswith('630')), 'Housing and community amenities')
                .when((col('FUNCTION2').startswith('320') | col('FUNCTION2').startswith('330') | col('FUNCTION2').startswith('390')), 'Social protection')
                .otherwise('General public services')
            ).withColumn('econ_sub',
                when((~col('is_transfer')) & col('ECON4').startswith('100') & 
                      (col('ECON5').startswith('100') | col('ECON5').startswith('110') |
                       col('ECON5').startswith('120') | col('ECON5').startswith('140') | col('ECON5').startswith('160')), 'basic wages')
                .when((~col('is_transfer')) & col('ECON4').startswith('100') & 
                      (col('ECON5').startswith('130') | col('ECON5').startswith('180') | col('ECON5').startswith('190')), 'allowances')
                .when(col('ECON5').startswith('210'), 'basic services')
                .when(col('ECON5').startswith('260'), 'employment contracts')
                .when(col('ECON5').startswith('240') & col('ECON2').startswith('120'), 'recurrent maintenance')
                .when((~col('is_transfer')) & col('FUNCTION2').startswith('320') , 'social assistance')
                .when((~col('is_transfer')) & col('ECON5').startswith('820'), 'pensions')
            ).withColumn('econ',
                # wage bill
                when((col('ECON4').startswith('100') & (~col('is_transfer'))), 'Wage bill')
                # capital expenditure
                .when((col('ECON1').startswith('200') & (~col('is_transfer')) &
                    (~col('ECON4').startswith('100')) &
                    (~col('ECON4').startswith('200')) &
                    (~col('ECON4').startswith('300')) &
                    (~col('ECON6').startswith('871')) &
                    (~col('ECON6').startswith('876')) &
                    (~col('ECON6').startswith('879')) &
                    (~col('FUNCTION2').startswith('320'))), 'Capital expenditures')
                # goods and services
                .when(((~col('is_transfer')) & (~col('FUNCTION2').startswith('320')) & (col('ECON4').startswith('200') | col('ECON4').startswith('300'))), 'Goods and services')
                # subsidies 
                .when(((~col('is_transfer')) & (col('ECON6').isNotNull()) &(col('ECON6').startswith('871') | col('ECON6').startswith('876') | col('ECON6').startswith('879'))), 'Subsidies')
                # social benefits
                .when(col('econ_sub').isin(['social assistance', 'pensions']), 'Social benefits')
                # interest on debt
                .when((~col('is_transfer')) & ((col('ECON5').startswith('710')) | (col('ECON5').startswith('720'))), 'Interest on debt')
                .otherwise('Other expenses')
            ).select(
                col('YEAR').alias('year'),
                col('APPROVED').alias('approved'),
                col('MODIFIED').alias('revised'),
                col('COMMITTED').alias('executed'), 
                col('geo1_tmp').alias('geo1'),
                'is_transfer', 'is_foreign', 'adm1_name', 'admin0', 'admin1', 'admin2', 'ECON4', 'ECON5', 'FUNCTION2',  'func', 'func_sub', 'econ', 'econ_sub', 'sheet'
            )
    )

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'pry_boost_bronze_municipal')
def boost_bronze_municipal():
    # Load the data from CSV
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Municipalidades.csv')
            .withColumn('sheet', lit('municipal'))
            .filter(~col('ECON4').startswith('600') &
                    ~col('ECON5').startswith('730') &
                    ~col('ECON5').startswith('740')
            ).withColumn('admin0', lit('Regional')
            ).withColumn('admin1', initcap(trim(regexp_replace('geo', "^[\d\s-]+", '')))
            ).withColumn('admin2', initcap(trim(regexp_replace('geo', "^[\d\s-]+", '')))
            ).withColumn('geo1_tmp', initcap(trim(regexp_replace('geo', "^[\d\s-]+", '')))
            ).withColumnRenamed("approved", "APPROVED"
            ).withColumn('econ_sub',
                when(col('ECON5').startswith('210'), 'basic services')
                .when(col('ECON5').startswith('260'), 'employment contracts')
                .when(col('ECON5').startswith('870'), 'subsidies to production')
            ).withColumn( 'econ',
                # wage bill
                when(col('ECON4').startswith('100'), 'Wage bill')
                # capital expenditure
                .when(((col('ECON4').startswith('400') | col('ECON4').startswith('500') | col('ECON4').startswith('600')) |
                    ((~col('ECON4').startswith('100')) & (
                        col('ECON5').startswith('860') |
                        col('ECON5').startswith('870') | 
                        col('ECON5').startswith('880') |
                        col('ECON5').startswith('890') | 
                        col('ECON5').startswith('980'))
                    )),  'Capital expenditure')
                # goods and services
                .when((col('ECON4').startswith('200') | col('ECON4').startswith('300')), 'Goods and services')
                # subsidies
                .when(col('ECON5').startswith('870'), 'Subsidies')
                # interest on debt
                .when(((col('ECON5').startswith('710')) | (col('ECON5').startswith('720'))), 'Interest on debt')
                # other expenses
                .otherwise('Other expenses')
            ).withColumn('is_transfer', lit(None).cast("boolean")
            ).withColumn('is_foreign', lit(None).cast("boolean")
            ).withColumn('func', lit(None).cast("string")
            ).withColumn('func_sub', lit(None).cast("string")
            ).withColumn('adm1_name', lit(None).cast("string")
            ).withColumn('FUNCTION2', lit(None).cast("string")
            ).select(
                col('YEAR').alias('year'),
                'approved',
                col('MODIFIED').alias('revised'),
                col('PAID').alias('executed'),
                col('geo1_tmp').alias('geo1'),
                'is_transfer', 'is_foreign', 'adm1_name', 'admin0', 'admin1', 'admin2','ECON4', 'ECON5', 'FUNCTION2', 'func', 'func_sub', 'econ', 'econ_sub', 'sheet'             
            )
    )

@dlt.table(name='pry_boost_silver')
def boost_silver():
    cen = dlt.read('pry_boost_bronze_cen' )
    municipalidades = dlt.read('pry_boost_bronze_municipal')
    df = cen.union(municipalidades)
    return df
        
@dlt.table(name=f'pry_boost_gold')
def boost_gold():
    return (dlt.read(f'pry_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                'revised',
                'executed',
                'adm1_name',
                'is_transfer',
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

