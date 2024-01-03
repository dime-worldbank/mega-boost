# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper

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
    .filter(~(col('econ2') == "Adquisición de Activos Financieros"))
    .withColumn('pension',
      upper(col("func1")).like('%PENSIONES%') | upper(col("econ3")).like('%(DE PENSIONES)%')
    )
    .withColumn('is_transfer',
      ((col('econ3').startswith('03-03-05-001')) |
      (col('econ3').startswith('03-03-05-002'))) # These need to be included in functional calculations but excluded for total expenditure as functional calculations are only done at central level
    )
    .withColumn('func_sub',
      when(
        col("func1").isin("RAMA JUDICIAL", "JUSTICIA Y DEL DERECHO") , "judiciary"
      ).when(
        (col("admin1").startswith("151100") |
         col("admin1").startswith("151201") |
         col("admin1").startswith("151600") |
         col("admin1").startswith("160101") |
         col("admin1").startswith("160102")), "public safety"
      )
    )
    .withColumn('func',
      when(
        col("func_sub").isin("judiciary", "public safety") , "Public order and safety"
      ).when(
        col("func1") == "DEFENSA Y POLICÍA", "Defense" # important for this to be after "Public order and safety" to exclude those line items
      ).when(
        col("func1").isin(
          "AGRICULTURA Y DESARROLLO RURAL", 
          "TRANSPORTE",
          "MINAS Y ENERGÍA",
          "TECNOLOGÍAS DE LA INFORMACIÓN Y LAS COMUNICACIONES",
          "COMERCIO, INDUSTRIA Y TURISMO",
          "EMPLEO PÚBLICO",
        ), "Economic affairs"
      ).when(
        col("func1") == "AMBIENTE Y DESARROLLO SOSTENIBLE", "Environmental protection"
      ).when(
        col("func1") == "VIVIENDA, CIUDAD Y TERRITORIO", "Housing and community amenities"
      ).when(
        (col("admin1").startswith("1912") |
         col("admin1").startswith("1913") |
         col("admin1").startswith("1914")), "Social protection" 
         # In excel there is overlap between SP & health, it's removed here
      ).when(
        col("func1") == "SALUD Y PROTECCIÓN SOCIAL", "Health" # important for this to be after Social protection
      ).when(
        col("func1").isin(
          "CULTURA", 
          "DEPORTE Y RECREACIÓN"
        ), "Recreation, culture and religion"
      ).when(
        col("func1") == "EDUCACIÓN", "Education"
      ).otherwise(
        lit("General public services")
      )
    )
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
    .select('country_name',
            'adm1_name',
            'year',
            col('ApropiacionDefinitiva').alias('approved'),
            col('Compromiso').alias('revised'),
            col('Pago').alias('executed'),
            'is_transfer',
            'func')
    .union(dlt.read('col_subnat_boost_silver')
      .withColumn('country_name', lit(COUNTRY))
      .withColumn('revised', lit(None))
      .select('country_name',
              'adm1_name',
              col('Ano').alias('year'),
              col('Approved').alias('approved'),
              'revised',
              col('Executed').alias('executed'))
      .withColumn('is_transfer', lit(False))
      .withColumn('func', lit("General public services"))
    )
  )
