# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf
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

# COMMAND ----------

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
    # Load the bronze data
    bronze_df = dlt.read(f'pry_boost_bronze')

    # Define the standardization function
    def clean_province_name(name):
        if not name:
            return
        not_province = ['Alcance Nacional', 'Auxiliar Traspaso', 'No Disponible']
        nfkd_form = unicodedata.normalize('NFKD', name)
        name = ''.join([c for c in nfkd_form if not unicodedata.combining(c)]).split(' - ')[-1].strip().title()
        # dont return name when it's not a province
        if name in not_province:
            return
        return name
    
    # Register the UDF
    clean_province_udf = udf(clean_province_name, StringType())

    # Add a new column 'admin1_name' using the clean_province_name function
    silver_df = bronze_df.withColumn('adm1_name', clean_province_udf('GEO1'))
    return silver_df

    
@dlt.table(name=f'pry_boost_gold')
def boost_gold():
    return (dlt.read(f'pry_boost_silver')
        .filter((col('ECON4') != '600 - Inversión Financiera') &
                (col('ECON5') != '730 - Amortización de la Deuda Pública Interna') &
                (col('ECON5') != '740 - Amortización de la Deuda Pública Externa'))
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                col('YEAR').alias('year'),
                'adm1_name',
                col('APPROVED').alias('approved'),
                col('PAID').alias('executed'))
    )
