# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, trim, regexp_replace 

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Mozambique'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'
AUXILIARY_CSV = f'{WORKSPACE_DIR}/auxiliary_csv/Mozambique/adm5.csv'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.table(name=f'moz_adm5_master_key_bronze')
def adm5_master_key_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(AUXILIARY_CSV)
    )

@dlt.expect_or_drop("exp_type_not_null", "ExpType IS NOT NULL")
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'moz_boost_bronze')
def boost_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(COUNTRY_MICRODATA_DIR)
    )

@dlt.table(name=f'moz_boost_silver')
def boost_silver():
    # take the opportunity to make Adm5 consistent - unify with/without accent, upper/lower case
    return (dlt.read(f'moz_boost_bronze')
        .select("*",
                substring('Adm5', 1, 1).alias("UGB_third")
               )
        .join(dlt.read(f'moz_adm5_master_key_bronze'), ["UGB_third"], "left")
        .drop('Adm5')
        .select("*", col('Adm51').alias('Adm5'))
        .drop('Adm51', 'UGB_third')
        .withColumn('adm1_name',
                    when(
                        col("Adm5En") == "Maputo (city)", "Cidade de Maputo"
                    ).otherwise(
                        when(
                            col("Adm5En") == "Central", "Central Scope"
                        ).otherwise(col("Adm5En"))
                    ))
        .withColumn('admin2',
            trim(regexp_replace(col("Adm2"), '^[0-9\\s]*', ''))
        )
        .withColumn('func_sub',
            when(
                col("Func1").startswith('03') & (col('Func2') == '03311 Tribunais') , "judiciary"
            ).when(
                col("Func1").startswith('03'), "public safety" # important for this to be after judiciary
            )
        )
        .withColumn('func',
            when(
                col('Func1').startswith("01"), "General public services"
            ).when(
                col('Func1').startswith("02"), "Defense"
            ).when(
                col("func_sub").isin("judiciary", "public safety") , "Public order and safety"
            ).when(
                col('Func1').startswith("04"), "Economic affairs"
            ).when(
                col('Func1').startswith("05"), "Environmental protection"
            ).when(
                col('Func1').startswith("06"), "Housing and community amenities"
            ).when(
                col('Func1').startswith("07"), "Health"
            ).when(
                col('Func1').startswith("08"), "Recreation, culture and religion"
            ).when(
                col('Func1').startswith("09"), "Education"
            ).when(
                col('Func1').startswith("10"), "Social protection"
            )
        )
    )
    
@dlt.table(name=f'moz_boost_gold')
def boost_gold():
    return (dlt.read(f'moz_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .withColumn('is_transfer', lit(False))
        .select('country_name',
                'adm1_name',
                col('Year').alias('year'),
                col('DotacaoInicial').alias('approved'),
                col('DotacaoActualizada').alias('revised'),
                col('Execution').alias('executed'),
                'admin2',
                'is_transfer',
                'func',
        )
    )
