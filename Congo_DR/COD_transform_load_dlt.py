# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Congo'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "Exercice IS NOT NULL")
@dlt.table(name=f'cod_boost_bronze')
def boost_bronze():
    bronze_df = (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(COUNTRY_MICRODATA_DIR)
    )
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name=f'cod_boost_silver')
def boost_silver():
    return (dlt.read(f'cod_boost_bronze')
        .withColumn('adm1_name', 
            when(trim(lower(col("Province"))) == "00 services centraux", 'Central Scope')
            .when((trim(lower(col("Province"))) == "19 multiprovince") | (trim(lower(col("Province"))) == "27 multi-province"), 'Other')
            .when(col("Province").isNotNull(),
                lower(trim(regexp_replace(col("Province"), "\d+", "")))
            )
            .otherwise('Other') 
        )
    )

@dlt.table(name=f'cod_boost_gold')
def boost_gold():
    return (dlt.read(f'cod_boost_silver')
            .filter(~(col('Titre') == "1 DETTE PUBLIQUE EN CAPITAL"))
            .withColumn('country_name', lit(COUNTRY))
            .select('country_name',
                    'adm1_name',
                    col('Exercice').alias('year'),
                    col('Montant_Voté').alias('approved'),
                     # Its unclear from the notes how Montant Vote, Montant Dotation, Montant ENgage, Montant Liquide and Montant ODFC relate to approved and committed
                    col('Montant_Engagé').alias('revised'),
                    col('Montant_Payé').alias('executed'),
                    )
    )