# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace, initcap

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/Congo'

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
                lower(trim(regexp_replace(col("Province"), "\d+", ""))))
            .otherwise('Other') 
        ).withColumn(
            'admin0', lit('Central')
        ).withColumn(
            'admin1', lit('Central')
        ).withColumn(
            'admin2', initcap(regexp_replace(col('Chapitre'), '^[0-9\\s]*', ''))
        ).withColumn(
            'geo1',
            when(col("Province").startswith('00'), 'Central Scope')
            .when((col("Province").startswith("19") | col("Province").startswith('27')), 'Central Scope')
            .when(col("Province").isNotNull(),
                initcap(trim(regexp_replace(col("Province"), "\d+", ""))))
        ).withColumn(
            'func_sub',
                # education breakdown
                # NOTE: post 2016 the code for primary education is not present. pre-primary is the only one tagged -- we assume it refers to primary as well following previous years codes
                when(
                    col('Sous_Fonction_').startswith('091'), 'primary education')
                .when(
                    col('Sous_Fonction_').startswith('092'), 'secondary education') 
                .when(
                    col('Sous_Fonction_').startswith('094'), 'tertiary education')
                # public safety
                .when(
                    col('Fonction2').startswith('03'), 'public safety' )
                # judiciary (SHOULD come after public safety)
                .when(
                    col('Fonction2').startswith('033'), 'judiciary')
                # No specific indicators for primary, secondary, tertiary or quaternary health
            ).withColumn(
                    'func',
                when(col('Grande_Fonction').startswith('01'), 'General public services')
                .when(col('Grande_Fonction').startswith('02'), 'Defence')
                .when(col('Grande_Fonction').startswith('03'), 'Public order and safety')
                .when(col('Grande_Fonction').startswith('04'), 'Economic affairs')
                .when(col('Grande_Fonction').startswith('05'), 'Environmental protection')
                .when(col('Grande_Fonction').startswith('06'), 'Housing and community amenities')
                .when(col('Grande_Fonction').startswith('07'), 'Health')
                .when(col('Grande_Fonction').startswith('08'), 'Recreation, culture and religion')              
                .when(col('Grande_Fonction').startswith('09'), 'Education')        
                .when(col('Grande_Fonction').startswith('10'), 'Social protection')
            ).withColumn('is_transfer', lit(False))
    )

@dlt.table(name=f'cod_boost_gold')
def boost_gold():
    return (dlt.read(f'cod_boost_silver')
            .filter(~(col('Titre') == "1 DETTE PUBLIQUE EN CAPITAL"))
            .withColumn('country_name', lit('Congo, Dem. Rep.')) 
            .select('country_name',
                    'adm1_name',
                    col('Exercice').alias('year'),
                    col('Montant_Voté').alias('approved'),
                     # Its unclear from the notes how Montant Vote, Montant Dotation, Montant ENgage, Montant Liquide and Montant ODFC relate to approved and committed
                    col('Montant_Engagé').alias('revised'),
                    col('Montant_Payé').alias('executed'),
                    'admin0',
                    'admin1',
                    'admin2',
                    'geo1',
                    'is_transfer',
                    'func',
                    'func_sub'
                    )
    )
