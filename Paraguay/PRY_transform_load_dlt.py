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

@dlt.table(name=f'pry_boost_bronze')
def boost_bronze():
    # Load the data from CSV
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(COUNTRY_MICRODATA_DIR))
    return bronze_df

@dlt.table(name=f'pry_boost_silver')
def boost_silver():
    return (dlt.read(f'pry_boost_bronze')
        .withColumn('adm1_name_tmp', 
                    initcap(trim(regexp_replace(col("GEO1"), "^.+-", "")))
        )
        .withColumn('adm1_name',
                    when(col("adm1_name_tmp") == 'Alcance Nacional', 'Central Scope')
                    .when(col("adm1_name_tmp") == 'Auxiliar Traspaso', 'Auxiliary Transfer')
                    .when(col("adm1_name_tmp") == 'No Disponible', None)
                    .otherwise(col("adm1_name_tmp"))
        )
        .drop('adm1_name_tmp')
        .withColumn('func_sub',
            when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 col('FUNCTION2').startswith('120') &
                 col('TRANSFER').startswith('TRANS')),
                "judiciary"
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 col('FUNCTION2').startswith('220') &
                 col('TRANSFER').startswith('TRANS')),
                "public safety"
            ))
        .withColumn('func', 
            when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 (col('FUNCTION1').startswith('100') | col('FUNCTION1').startswith('500')) &
                 ~col('TRANSFER').startswith('TRANS')),
                'General public services'          
            ).when(
                col("func_sub").isin("judiciary", "public safety"),
                "Public order and safety"
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 col('FUNCTION2').startswith('210') &
                 ~col('TRANSFER').startswith('TRANS')),
                'Defence'                
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 (col('FUNCTION1').startswith('400') | col('FUNCTION1').startswith('600')) &
                 ~col('TRANSFER').startswith('TRANS')),
                'Economic affairs' 
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 col('FUNCTION2').startswith('310') &
                 ~col('TRANSFER').startswith('TRANS')),
                'Health' 
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 (col('FUNCTION3').startswith('344') | col('FUNCTION3').startswith('345')) &
                 ~col('TRANSFER').startswith('TRANS')),
                'Recreation, culture and religion' 
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 col('FUNCTION2').startswith('340') &
                 ~col('TRANSFER').startswith('TRANS')),
                'Education'   
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 col('FUNCTION2').startswith('440') &
                 ~col('TRANSFER').startswith('TRANS')),
                'Environmental protection'   
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 (col('FUNCTION2').startswith('370') | col('FUNCTION2').startswith('380') | col('FUNCTION2').startswith('630')) &
                 ~col('TRANSFER').startswith('TRANS')),
                'Housing and community amenities'
            ).when(
                (~col('ECON4').startswith('600') &
                 ~col('ECON5').startswith('730') &
                 ~col('ECON5').startswith('740') &
                 (col('FUNCTION2').startswith('320') | col('FUNCTION2').startswith('330') | col('FUNCTION2').startswith('390')) &
                 ~col('TRANSFER').startswith('TRANS')),
                'Social protection'
            )
        )
    )
    
@dlt.table(name=f'pry_boost_gold')
def boost_gold():
    return (dlt.read(f'pry_boost_silver')
        .filter(~(col('ECON4').startswith("600 -") |
                  col('ECON5').startswith("730 -") |
                  col('ECON5').startswith('740 -')))
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'adm1_name',
                col('YEAR').alias('year'),
                col('APPROVED').alias('approved'),
                col('MODIFIED').alias('revised'),
                col('PAID').alias('executed'),
                'func')
    )
