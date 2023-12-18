# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Colombia'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.expect_or_drop("econ1_valid", "econ1 IN ('Funcionamiento', 'Inversion')")
@dlt.table(name=f'col_central_boost_bronze')
def central_boost_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/central.csv')
    )

@dlt.expect_or_drop("year_not_null", "Ano IS NOT NULL")
@dlt.table(name=f'col_subnat_boost_bronze')
def subnat_boost_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/subnational.csv')
    )

@dlt.table(name=f'col_central_boost_silver')
def central_boost_silver():
  return (dlt.read('col_central_boost_bronze')
    .filter(~((col('econ2') == "Adquisición de Activos Financieros") |
              (col('econ3').startswith('03-03-05-001')) |
              (col('econ3').startswith('03-03-05-002'))
             ))
  )

@dlt.expect_or_drop("adm1_name_not_null", "adm1_name IS NOT NULL")
@dlt.table(name=f'col_subnat_boost_silver')
def subnat_boost_silver():
  adm_lookup = spark.table('indicator_intermediate.col_subnational_adm2_adm1_lookup')
  return (dlt.read('col_subnat_boost_bronze')
    .select('*', element_at(split('admin1', ' ', 2), 1).cast('integer').alias("nso_code"))
    .join(adm_lookup, on=["nso_code"], how="left")
  )

@dlt.table(name='col_boost_gold')
def boost_gold():
  return(dlt.read('col_central_boost_silver')
    .withColumn('country_name', lit(COUNTRY))
    .withColumn('adm1_name', lit('Central Scope'))
    .select('year',
            'country_name',
            'adm1_name',
            col('ApropiacionDefinitiva').alias('approved'),
            col('Compromiso').alias('revised'),
            col('Pago').alias('executed'))
    .union(dlt.read('col_subnat_boost_silver')
      .withColumn('country_name', lit(COUNTRY))
      .withColumn('revised', lit(None))
      .select(col('Ano').alias('year'),
              'country_name',
              'adm1_name',
              col('Approved').alias('approved'),
              'revised',
              col('Executed').alias('executed'))
    )
  )
