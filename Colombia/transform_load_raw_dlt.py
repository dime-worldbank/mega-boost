# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, length, lead, expr, trim, lpad, concat, last
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

## Subnational ##

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

@dlt.table(name='col_subnat_econ1')
def col_subnat_econ1():
    return (dlt.read('col_subnat_recent_bronze')
      .filter(length(col("concepto_cod")) == 3)
      .select(
        "year",
        col("concepto_cod").alias("concepto_cod_first3"),
        col("nombreconcepto").alias("econ1")
      )
      .distinct()
    )

@dlt.table(name='col_subnat_econ2')
def col_subnat_econ2():
    return (dlt.read('col_subnat_recent_bronze')
      .filter(length(col("concepto_cod")) == 5)
      .select(
        "year",
        col("concepto_cod").alias("concepto_cod_first5"),
        col("nombreconcepto").alias("econ2")
      )
      .distinct()
    )

@dlt.table(name='col_subnat_econ3')
def col_subnat_econ3():
    return (dlt.read('col_subnat_recent_bronze')
      .filter(length(col("concepto_cod")) == 8)
      .select(
        "year",
        col("concepto_cod").alias("concepto_cod_first8"),
        col("nombreconcepto").alias("econ3")
      )
      .distinct()
    )

@dlt.table(name='col_subnat_econ4')
def col_subnat_econ4():
    return (dlt.read('col_subnat_recent_bronze')
      .filter(length(col("concepto_cod")) == 11)
      .select(
        "year",
        col("concepto_cod").alias("concepto_cod_first11"),
        col("nombreconcepto").alias("econ4")
      )
      .distinct()
    )

@dlt.table(name='col_subnat_econ5')
def col_subnat_econ5():
    return (dlt.read('col_subnat_recent_bronze')
      .filter(length(col("concepto_cod")) == 15)
      .select(
        "year",
        col("concepto_cod").alias("concepto_cod_first15"),
        col("nombreconcepto").alias("econ5")
      )
      .distinct()
    )

@dlt.expect_or_drop("concepto_cod_not_null", "concepto_cod IS NOT NULL")
@dlt.expect_or_drop("econ1_not_null", "econ1 IS NOT NULL")
@dlt.expect_or_drop("econ2_not_null", "econ2 IS NOT NULL")
@dlt.expect_or_drop("econ3_not_null", "econ3 IS NOT NULL")
@dlt.table(name='col_subnat_recent_silver')
def col_subnat_recent_silver():
    # Add econ1 to econ5
    with_econ = (dlt.read('col_subnat_recent_bronze')
      .select('*',
        substring('concepto_cod', 1, 3).alias("concepto_cod_first3"),
        substring('concepto_cod', 1, 5).alias("concepto_cod_first5"),
        substring('concepto_cod', 1, 8).alias("concepto_cod_first8"),
        substring('concepto_cod', 1, 11).alias("concepto_cod_first11"),
        substring('concepto_cod', 1, 15).alias("concepto_cod_first15"),
      )
      .join(dlt.read('col_subnat_econ1'), on=["year", "concepto_cod_first3"], how="left")
      .join(dlt.read('col_subnat_econ2'), on=["year", "concepto_cod_first5"], how="left")
      .join(dlt.read('col_subnat_econ3'), on=["year", "concepto_cod_first8"], how="left")
      .join(dlt.read('col_subnat_econ4'), on=["year", "concepto_cod_first11"], how="left")
      .join(dlt.read('col_subnat_econ5'), on=["year", "concepto_cod_first15"], how="left")
      .select('*',
        concat(col("concepto_cod_first3"), lit(' '), col("econ1")).alias("econ1_w_code"),
        concat(col("concepto_cod_first5"), lit(' '), col("econ2")).alias("econ2_w_code"),
        concat(col("concepto_cod_first8"), lit(' '), col("econ3")).alias("econ3_w_code"),
        concat(col("concepto_cod_first11"), lit(' '), col("econ4")).alias("econ4_w_code"),
        concat(col("concepto_cod_first15"), lit(' '), col("econ5")).alias("econ5_w_code"),
      )
    )

    # Add adm1 and adm2 name columns
    adm_lookup = spark.table('indicator_intermediate.col_subnational_adm2_adm1_lookup')
    with_geo = (with_econ
      .withColumn(
        "nso_code", expr("substring(codigofut, length(codigofut)-4, 5)").cast('integer'))
      .join(adm_lookup, on=["nso_code"], how="left")
    )

    # Add func1 column
    with_func = (with_geo.withColumn(
      'entitad_cod', lpad(col('seccionpresupuestal').cast('int'), 2, '0')
    ).withColumn(
      'func1',
      when(col('entitad_cod').isin(['02', '21']), "Salud")
      .when(col('entitad_cod') == '24', "Pensions")
      .when(col('entitad_cod') == '22', "Education")
      .otherwise("General Services")
    ))

    # Add a flag to indicate if a row is a line item or aggregate entry
    return (with_func
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

# COMMAND ----------

## Central ##

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'col_central_bronze')
def col_central_bronze():
    windowSpec = Window.partitionBy("year").orderBy('raw_row_id')
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/central_execution_*.csv')
      .withColumn( # Add a new column that references the next row's value
        "entidad_next_row", lead(col("EntidadDetalle"), offset=1).over(windowSpec))
      .withColumn( # Add a new column that references the next next row's value
        "entidad_next_next_row", lead(col("EntidadDetalle"), offset=2).over(windowSpec))
    )

@dlt.expect_or_drop("econ1_not_null", "econ1 IS NOT NULL")
@dlt.expect_or_drop("econ2_not_null", "econ2 IS NOT NULL")
@dlt.expect_or_drop("econ3_not_null", "econ3 IS NOT NULL")
@dlt.expect_or_drop("EntidadDetalle", "EntidadDetalle IS NOT NULL")
@dlt.expect_or_drop("func1_not_null", "func1 IS NOT NULL")
@dlt.expect_or_drop("admin1_not_null", "admin1 IS NOT NULL")
@dlt.table(name='col_central_silver')
def col_central_silver():
    windowSpec = Window.partitionBy("year").orderBy('raw_row_id')
    sparse_df = (dlt.read('col_central_bronze')
        .withColumn('func1', when((col('entidad_next_next_row') == "Funcionamiento") & ~col('EntidadDetalle').rlike("^\d"), col('EntidadDetalle')))
        .withColumn('admin1', when(col('entidad_next_row') == "Funcionamiento", col('EntidadDetalle')))
        .withColumn(
            "econ1",
            when(substring('EntidadDetalle', 3, 1) == '-', "Funcionamiento")
            .when(substring('EntidadDetalle', 5, 1) == '-', "Inversion"))
        .withColumn(
            "econ3",
            when(col("econ1").isNotNull(), col("EntidadDetalle")))
        .withColumn(
            "econ3_next_row",
            lead(col("econ3")).over(windowSpec))
        .withColumn(
            "econ2",
            when(col("econ3").isNull() & col("econ3_next_row").isNotNull(), col("EntidadDetalle"))
        )
    )
    
    return (sparse_df
        .withColumn("func1", last(col("func1"), ignorenulls=True).over(windowSpec))
        .withColumn("admin1", last(col("admin1"), ignorenulls=True).over(windowSpec))
        .withColumn("econ2", last(col("econ2"), ignorenulls=True).over(windowSpec))
    )

@dlt.table(name='col_central_gold')
def col_central_gold():
    return (dlt.read('col_central_silver')
        .select('year', 'func1', 'admin1', 'econ1', 'econ2', 'econ3',
                'ApropiacionDefinitiva', 'Compromiso', 'Obligacion', 'Pago')
    )
