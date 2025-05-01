# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf, trim, regexp_replace, initcap, concat, lower
from pyspark.sql.types import StringType, DoubleType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
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
            .withColumn(
                'is_transfer', col('TRANS').isNotNull()                        
            ).withColumn(
                'is_foreign', (~col('is_transfer')) & (col('Foreign')=='Foreign')
            ).withColumn(
                'admin0', lit('Central')
            ).withColumn(
                'admin1', lit('Central Scope')
            ).withColumn(
                'admin2', initcap(trim(regexp_replace('ADMIN1', "^[\d\s-]+", '')))
            ).withColumn(
                'geo1_tmp', initcap(trim(regexp_replace(col("GEO1"), "^.+-", ""))) 
            ).withColumn('geo1',
                when(col("geo1_tmp").isin([
                    'Alcance Nacional',
                    'Auxiliar Traspaso',
                    'No Disponible',
                    ]), 'Central Scope'
                ).otherwise(col("geo1_tmp"))
            ).drop(
                'geo1_tmp'
            ).withColumn('func_sub',
                when(col('FUNCTION2').startswith('120'), "Judiciary")
                .when(col('FUNCTION2').startswith('220'), "Public Safety")
                .when((col('FUNCTION3').startswith('311')) & ((col('PROGRAM1').startswith('001') | col('PROGRAM1').startswith('007')) & (col('hospital').isNull())), "Primary and Secondary Health")
                .when((col('FUNCTION2').startswith('310')) & (col('ECON2').startswith('120') & col('ECON5').startswith('240')), "Tertiary and Quaternary Health")
                .when(col('FUNCTION3').startswith('341'), "Primary Education")
                .when(col('FUNCTION3').startswith('342'), "Secondary Education") # No further information about higher education 
            ).withColumn('func', 
                when(col("func_sub").isin("Judiciary", "Public Safety"), "Public order and safety")
                .when(col('FUNCTION2').startswith('210'), 'Defence')
                .when(col('FUNCTION2').startswith('440'), 'Environmental protection') # env needs to be before economic affairs due to overlap
                .when(col('FUNCTION2').startswith('310'), 'Health')
                .when((lower(col('FUNCTION3')).startswith('344 - cultura') | lower(col('FUNCTION3')).startswith('345 - deporte y recreacion')), 'Recreation, culture and religion')
                .when(lower(col('FUNCTION2')).startswith('340'), 'Education')
                .when((col('FUNCTION2').startswith('370') | col('FUNCTION2').startswith('380') | col('FUNCTION2').startswith('630')), 'Housing and community amenities')
                .when((col('FUNCTION2').startswith('320') | col('FUNCTION2').startswith('330') | lower(col('FUNCTION2')).startswith('390 - otros servicios sociales')), 'Social protection')
                .when((col('FUNCTION1').startswith('400') | col('FUNCTION1').startswith('600')), 'Economic affairs')
                .otherwise('General public services')
            ).withColumn('econ_sub',
                when((~col('is_transfer')) & col('FUNCTION2').startswith('320') , 'Social Assistance')
                .when((~col('is_transfer')) & col('ECON5').startswith('820'), 'Pensions')
                .when((~col('is_transfer')) & col('ECON4').startswith('100') & 
                      (col('ECON5').startswith('100') | col('ECON5').startswith('110') |
                       col('ECON5').startswith('120') | col('ECON5').startswith('140') | col('ECON5').startswith('160')), 'Basic Wages')
                .when((~col('is_transfer')) & col('ECON4').startswith('100') & 
                      (col('ECON5').startswith('130') | col('ECON5').startswith('180') | col('ECON5').startswith('190')), 'Allowances')
                .when(col('ECON5').startswith('210'), 'Basic Services')
                .when(col('ECON5').startswith('260'), 'Employment Contracts')
                .when(col('ECON5').startswith('240') & col('ECON2').startswith('120'), 'Recurrent Maintenance')
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
                .when(col('econ_sub').isin(['Social Assistance', 'Pensions']), 'Social benefits')
                # interest on debt
                .when((~col('is_transfer')) & ((col('ECON5').startswith('710')) | (col('ECON5').startswith('720'))), 'Interest on debt')
                .otherwise('Other expenses')
            ).select(
                col('YEAR').alias('year'),
                col('APPROVED').alias('approved').cast(DoubleType()),
                col('MODIFIED').alias('revised').cast(DoubleType()),
                col('COMMITTED').alias('executed').cast(DoubleType()), 
                'geo1',
                'is_transfer', 'is_foreign', 'admin0', 'admin1', 'admin2', 'ECON1', 'ECON2', 'ECON4', 'ECON5', 'ECON6', 'FUNCTION2', 'FUNCTION3','PROGRAM1', 'hospital', 'func', 'func_sub', 'econ', 'econ_sub', 'sheet'
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
            ).withColumn('admin2', col('admin1')
            ).withColumn('geo1', col('admin1')
            ).withColumnRenamed("approved", "APPROVED"
            ).withColumn('econ_sub',
                when(col('ECON5').startswith('210'), 'Basic Services')
                .when(col('ECON5').startswith('260'), 'Employment Contracts')
                .when(col('ECON5').startswith('870'), 'Subsidies to Production')
            ).withColumn( 'econ',
                # wage bill
                when(col('ECON4').startswith('100'), 'Wage bill')
                # subsidies
                .when(col('ECON5').startswith('870'), 'Subsidies')
                # capital expenditure
                .when(((col('ECON4').startswith('400') | col('ECON4').startswith('500') | col('ECON4').startswith('600')) |
                    ((~col('ECON4').startswith('100')) & (
                        col('ECON5').startswith('860') |
                        col('ECON5').startswith('870') | 
                        col('ECON5').startswith('880') |
                        col('ECON5').startswith('890') | 
                        col('ECON5').startswith('980'))
                    )),  'Capital expenditures')
                # goods and services
                .when((col('ECON4').startswith('200') | col('ECON4').startswith('300')), 'Goods and services')
                # interest on debt
                .when(((col('ECON5').startswith('710')) | (col('ECON5').startswith('720'))), 'Interest on debt')
                # other expenses
                .otherwise('Other expenses')
            ).withColumn('is_transfer', lit(None).cast("boolean")
            ).withColumn('is_foreign', lit(None).cast("boolean")
            ).withColumn('func', lit("General public services")
            ).withColumn('func_sub', lit(None).cast("string")
            ).withColumn('FUNCTION2', lit(None).cast("string")
            ).withColumn('ECON1', lit(None).cast("string")
            ).withColumn('ECON2', lit(None).cast("string")
            ).withColumn('FUNCTION3', lit(None).cast("string")
            ).withColumn('PROGRAM1', lit(None).cast("string")
            ).withColumn('hospital', lit(None).cast("string")
            ).select(
                col('YEAR').alias('year'),
                col('approved').cast(DoubleType()),
                col('MODIFIED').alias('revised').cast(DoubleType()),
                col('PAID').alias('executed').cast(DoubleType()),
                'geo1',
                'is_transfer', 'is_foreign', 'admin0', 'admin1', 'admin2', 'ECON1', 'ECON2', 'ECON4', 'ECON5', 'ECON6', 'FUNCTION2', 'FUNCTION3','PROGRAM1', 'hospital',  'func', 'func_sub', 'econ', 'econ_sub', 'sheet'             
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
