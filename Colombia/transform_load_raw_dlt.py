# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at,\
  split, upper, length, lead, expr, trim, lpad, concat, concat_ws, last,\
  coalesce, size, count, sum, lower
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

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

## Subnational: 2021 onwards ##

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
        "nso_code_padded", expr("substring(codigofut, length(codigofut)-4, 5)").cast('integer'))
      .withColumn(
        "nso_code", 
        when(col("nso_code_padded")%1000 == 0, col("nso_code_padded")/1000)
        .otherwise(col("nso_code_padded"))
        .cast('integer'))
      .join(adm_lookup, on=["nso_code"], how="left")
    )

    # Add func1 column
    with_func = (with_geo.withColumn(
      'entitad_cod', lpad(col('seccionpresupuestal').cast('int'), 2, '0')
    ).withColumn(
      'func1',
      when(col('entitad_cod').isin(['02', '21']), "Health")
      .when(col('entitad_cod') == '24', "Pensions")
      .when(col('entitad_cod') == '22', "Education")
      .otherwise("General public services")
    ))

    # Interpolate missing Pagos values
    with_pagos_interpolated = (with_func
      .withColumn(
        'pagos_interpolated',
        when(col("obligaciones").isNotNull() & col("pagos").isNull(), col("obligaciones"))
        .otherwise(col("pagos"))
      )
    )

    with_admin0 = (with_pagos_interpolated
      .withColumn('nombreentidad_lower', lower(col('nombreentidad')))
      .withColumn(
        'admin0',
        when(col('year') == 2021, # Special handling for 2021 subnational
          when(col('nombreentidad_lower').contains("hospital"), 'Hospital')
          .when(col('nombreentidad_lower').contains("universidad"), 'Universidad')
          .when(col('nombreentidad_lower').contains("departament"), 'Departamento')
          .when(col('codigofut').startswith('21') & (~col('nombreentidad_lower').contains("corporaci")), 'Municipalidad')
          .otherwise(lit('Other')))
        .otherwise("Regional")
      )
    )

    # Add a flag to indicate if a row is a line item or aggregate entry
    return (with_admin0
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

## Subnational: 2020 and before ##

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'col_subnat_2020_and_prior_funcionamiento_bronze')
def col_subnat_2020_and_prior_funcionamiento_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/subnational_gastos_*_FUT_GastosFuncionamiento.csv')
      .withColumn("econ1", lit("FUNCIONAMIENTO"))
    )

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'col_subnat_2020_and_prior_inversion_bronze')
def col_subnat_2020_and_prior_inversion_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/subnational_gastos_*_FUT_GastosInversion.csv')
      .withColumn("econ1", lit("INVERSION"))
    )

@dlt.table(name='col_subnat_2020_and_prior_inversion_unidad_ejecutora_lookup')
def col_subnat_2020_and_prior_inversion_unidad_ejecutora_lookup():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/subnational_2020_and_prior_inversion_unidad_ejecutora_lookup.csv')
    )

col_order = ['year', 'CodDANEDepartamento', 'CodDANEMunicipio',
             'econ1', 'UnidadEjecutora', 'CodigoConcepto', 'Concepto', 'CodigoFuenteFinanciacion',
             'PresupuestoInicial', 'PresupuestoDefinitivo', 'Compromisos', 'Obligaciones', 'Pagos']
@dlt.table(name=f'col_subnat_2020_and_prior_bronze')
def col_subnat_2020_and_prior_bronze():
    execution_entity_lookup = dlt.read("col_subnat_2020_and_prior_inversion_unidad_ejecutora_lookup")
    inversion = (dlt.read('col_subnat_2020_and_prior_inversion_bronze')
        .withColumn("CodigoFuenteFinanciacion", col("CodigoFuentesDeFinanciacion"))
        .withColumn("FuenteFinanciacion", col("FuentesdeFinanciacion"))
        .withColumn("CodigoConcepto_group2_int", element_at(split('CodigoConcepto', '\\.'), 2).cast(IntegerType()))
        .join(execution_entity_lookup, on=["CodigoConcepto_group2_int"], how="left")
        .select(col_order)
    )
    return (dlt.read('col_subnat_2020_and_prior_funcionamiento_bronze')
        .select(col_order)
        .union(inversion)
        .filter(col('year') >= 2019) # No detailed central data available prior, so look no further back for subnational
        .filter(~(col("CodigoConcepto")=='VAL')) # exclude special budget code (summary?)
        .withColumn('CodigoConcepto_groups', split('CodigoConcepto', '\\.'))
    )

@dlt.table(name='col_subnat_econ2_2020_and_prior')
def col_subnat_econ2_2020_and_prior():
    return (dlt.read('col_subnat_2020_and_prior_bronze')
      .filter(size(col("CodigoConcepto_groups")) == 3) # CodigoConcepto that contains 2 dots
      .select(
        "year",
        col("CodigoConcepto").alias("CodigoConcepto_frist3_group"),
        col("Concepto").alias("econ2")
      )
      .distinct()
    )

@dlt.table(name='col_subnat_econ3_2020_and_prior')
def col_subnat_econ3_2020_and_prior():
    return (dlt.read('col_subnat_2020_and_prior_bronze')
      .filter(size(col("CodigoConcepto_groups")) == 4) # CodigoConcepto that contains 3 dots
      .select(
        "year",
        col("CodigoConcepto").alias("CodigoConcepto_frist4_group"),
        col("Concepto").alias("econ3")
      )
      .distinct()
    )

@dlt.table(name=f'col_subnat_2020_and_prior_silver')
def col_subnat_2020_and_prior_silver():
    # Add adm1 and adm2 name columns
    adm_lookup = spark.table('indicator_intermediate.col_subnational_adm2_adm1_lookup')
    with_geo = (dlt.read('col_subnat_2020_and_prior_bronze')
      .filter(col("CodigoFuenteFinanciacion").isNotNull()) # exclude unspecified source of funding
      .withColumn("nso_code", coalesce(col("CodDANEMunicipio"), col("CodDANEDepartamento")).cast('integer'))
      .join(adm_lookup, on=["nso_code"], how="left")
    )

    with_func = (with_geo
      .withColumn(
        'func1',
        when(col('UnidadEjecutora').isin(['Education', 'Health']), col('UnidadEjecutora')) # INVERSION
        .when(col('UnidadEjecutora').startswith('6'), "Education") # FUNCIONAMIENTO
        .when(col('UnidadEjecutora').startswith('7'), "Health") # FUNCIONAMIENTO
        .otherwise("General public services") # Catch all for both FUNCIONAMIENTO and INVERSION
      )
    )

    with_econ = (with_func
      .select('*',
        concat_ws('.',
          element_at(col("CodigoConcepto_groups"), 1),
          element_at(col("CodigoConcepto_groups"), 2), 
          element_at(col("CodigoConcepto_groups"), 3)).alias("CodigoConcepto_frist3_group"),
        concat_ws('.',
          col("CodigoConcepto_frist3_group"),
          element_at(col("CodigoConcepto_groups"), 4)).alias("CodigoConcepto_frist4_group"),
      )
      .join(dlt.read('col_subnat_econ2_2020_and_prior'), on=["year", "CodigoConcepto_frist3_group"], how="left")
      .join(dlt.read('col_subnat_econ3_2020_and_prior'), on=["year", "CodigoConcepto_frist4_group"], how="left")
      .withColumn(
        'econ4',
        when(size(col("CodigoConcepto_groups")) > 4, col('Concepto'))
      )
    )
    
    return with_econ

# COMMAND ----------

## Combine earlier and recent Subnational ##

@dlt.table(name='col_subnat_gold')
def col_subnat_gold():
  return (dlt.read('col_subnat_2020_and_prior_silver')
    .withColumn('econ5', lit(None))
    .select("year",
            col("adm1_name").alias("admin1"),
            col("adm2_name").alias("admin2"),
            "func1",
            "econ1", "econ2", "econ3", "econ4", "econ5",
            col("PresupuestoInicial").alias("compromisos"),
            col("PresupuestoDefinitivo").alias("obligaciones"),
            col("Pagos").alias("pagos"))
    .union(dlt.read('col_subnat_recent_silver')
        .filter(col('is_line_item') & (col('vigenciagasto')==1) & ~(col('admin0')=='Other'))
        .select("year",
            col("adm1_name").alias("admin1"),
            col("adm2_name").alias("admin2"),
            "func1",
            "econ1", "econ2", "econ3", "econ4", "econ5",
            "compromisos",
            "obligaciones",
            col("pagos_interpolated").alias("pagos"))
    )
  )

@dlt.expect_or_fail("row_count should match expected", "row_count = row_count_actual")
@dlt.expect_or_fail("approved_total should match expected", "approved_total = approved_total_actual")
@dlt.expect_or_fail("executed_total should match expected", "executed_total = executed_total_actual")
@dlt.table(name='col_subnat_quality')
def col_subnat_quality():
    expected_agg = (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/expected_subnational_by_year.csv')
    )
    subnat_agg = (dlt.read('col_subnat_gold')
      .groupBy('year')
      .agg(count('*').alias('row_count_actual'),
           sum('compromisos').alias('approved_total_actual'),
           sum('pagos').alias('executed_total_actual'))
      .join(expected_agg, on=['year'], how='left')
    )
    #TODO: check all expected years are present (left join above would not catch the case all years are missing from col_subnat_gold)

    return subnat_agg

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

# TODO: add central quality checks – idea: against official summary items

# COMMAND ----------

## Harmonize to BOOST – Central ##

@dlt.table(name=f'col_central_boost_silver_from_raw')
def col_central_boost_silver_from_raw():
  return (dlt.read('col_central_gold')
    .filter(~(col('econ2') == "Adquisición de Activos Financieros"))
    .filter(~col('econ3').startswith('03-03-05-001') & ~col('econ3').startswith('03-03-05-002'))
    .withColumn('pension',
      upper(col("func1")).like('%PENSIONES%') | upper(col("econ3")).like('%(DE PENSIONES)%')
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

# COMMAND ----------

## Harmonize to BOOST – Regional ##

@dlt.table(name=f'col_subnat_boost_silver_from_raw')
def col_subnat_boost_silver_from_raw():
    return (dlt.read('col_subnat_gold')
        .withColumn("func",
            when(col("func1") == "Pensions", lit("Social protection"))
            .otherwise(col("func1"))
        )
    )

# COMMAND ----------

## Combine BOOST Central & Regional ##

@dlt.table(name='col_boost_gold')
def col_boost_gold():
  return(dlt.read('col_central_boost_silver_from_raw')
    .select("*", col('admin1').alias('admin2'))
    .drop("admin1")
    .withColumn('country_name', lit(COUNTRY))
    .withColumn('admin0', lit('Central'))
    .withColumn('admin1', lit('Central Scope'))
    .withColumn('geo1', lit('Central Scope')) # Col central spending is not geo tagged
    .select('country_name',
            'year',
            'admin0',
            'admin1',
            'admin2',
            'geo1',
            'func',
            'func_sub',
            col('ApropiacionDefinitiva').alias('approved'),
            col('Pago').alias('executed'))
    .union(dlt.read('col_subnat_boost_silver_from_raw')
      .withColumn('country_name', lit(COUNTRY))
      .withColumn('revised', lit(None))
      .withColumn('func_sub', lit(None))
      .withColumn('admin0', lit("Regional"))
      .withColumn('geo1', col('admin1'))
      .select('country_name',
              'year',
              'admin0',
              'admin1',
              'admin2',
              'geo1',
              'func',
              'func_sub',
              col('compromisos').alias('approved'),
              col('pagos').alias('executed'))
    )
  )
