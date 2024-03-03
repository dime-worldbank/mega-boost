# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, length, lead, expr, trim
from pyspark.sql.window import Window

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Colombia'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'col_subnat_recent_bronze')
def col_subnat_recent_bronze():
    windowSpec = Window.partitionBy("year", "codigofut", "seccionpresupuestal", "vigenciagasto").orderBy('concepto_cod')
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/subnational_gastos_*_EJECUCION_DE_GASTOS.csv')
      .withColumn( # Add a new column that references the next row's value
        "concepto_cod_next_row", lead(col("concepto_cod")).over(windowSpec))
    )

@dlt.expect_or_drop("concepto_cod_not_null", "concepto_cod IS NOT NULL")
@dlt.table(name='col_subnat_recent_silver')
def col_subnat_recent_silver():
    # Add adm1 and adm2 name columns
    adm_lookup = spark.table('indicator_intermediate.col_subnational_adm2_adm1_lookup')
    df = (dlt.read('col_subnat_recent_bronze')
      .withColumn(
        "nso_code", expr("substring(codigofut, length(codigofut)-4, 5)").cast('integer'))
      .join(adm_lookup, on=["nso_code"], how="left")
    )

    # Add a flag to indicate if a row is a line item or aggregate entry
    return (df
      .withColumn(
        "overlap_next_row_all_but_last_4", expr("substring(concepto_cod_next_row, 1, length(concepto_cod_next_row) - 4) = concepto_cod"))
      .withColumn(
        "overlap_next_row_all_but_last_3", expr("substring(concepto_cod_next_row, 1, length(concepto_cod_next_row) - 3) = concepto_cod"))
      .withColumn( 
        "is_line_item",
        when(length(col("concepto_cod")) < 4, False)
        .when((length(col("concepto_cod_next_row")) == 15) & col("overlap_next_row_all_but_last_4"), False)
        .when(col("overlap_next_row_all_but_last_3"), False)
        .otherwise(True) #TODO check with Massimo: 2021 further filters by vigenciagasto = 1, while 2022 doesn't have this restriction
      )
    )
