# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, initcap, element_at, split, upper, trim, lower, regexp_replace, regexp_extract, substring, expr, concat

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/Bhutan'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'btn_boost_bronze')
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

@dlt.table(name=f'btn_boost_silver')
def boost_silver():
    return (dlt.read(f'btn_boost_bronze')
            .filter(~(col('Econ2').startswith('32')))
            # adm1_name
            .withColumn(
                'adm1_name',
                when(col("Admin2").startswith("4"), trim(regexp_replace(col("Admin2"), "\d+", "")))
                # manually checked that Mongar district is missing and corresponds to 414 (which is missing in the consecutive codes for the districts)
                .when((col("Admin2").isNull()) & (col("Admin3").startswith("414")), "Mongar")
                # imputing that the rest belongs to central scope (various ministries etc)
                .otherwise("Central Scope")
            ).withColumn(
                'admin0', 
                when(lower(col('Admin1'))=='central', 'Central')
                .when(lower(col('Admin1'))=='local', 'Regional')
                .otherwise('Regional') # correcting the null values manually
            ).withColumn(
                'admin1', 
                when(col('admin0')=='Central', 'Central')
                # error with one district 'Mongar'
                .when((col('Admin2').isNull()) & (col('Admin3').startswith("414")), "Mongar")
                .when(col('admin0')=='Regional', initcap(regexp_replace(col("Admin2"), '^[0-9\\s]*', '')))
            ).withColumn(
                'admin2', 
                when(col('admin1')=='Mongar', 'Mongar')
                .otherwise(initcap(trim(regexp_replace(col("Admin2"), '^[0-9\\s]*', ''))))
            ).withColumn(
                'func_sub',
                when(col('prog1').startswith('5 '), 'judiciary')
                .when(col('prog1').startswith('30'), 'public safety')
                # agriculture
                .when((col('prog1').startswith('43') | col('prog1').startswith('44') | col('prog1').startswith('45') | col('prog1').startswith('46') | col('prog1').startswith('48') | col ('prog1').startswith('83')), 'agriculture')
                # transportation
                .when(
                    (((col('airport')==1) & (~col('prog1').startswith('33')) & (~col('prog1').startswith('55'))) |
                    (col('Roads')==True)), 'transport')
                # education spending decomposed
                .when(col('prog2').startswith('87'), 'primary education')
                .when((col('prog2').startswith("88") | col('prog2').startswith("89") | col("prog2").startswith("90")), "secondary education")
                .when((col("prog1").startswith("16") | col("prog1").startswith("15")),  "tertiary education")
                # health spending breakdown       
                .when(col('prog1').startswith('69'), 'primary and secondary health')
                .when(col('prog1').startswith('68'), 'tertiary and quaternary health')
            ).withColumn(
                'func',
                when(((col('Econ3')=='Social benefits') | col('econ4').startswith('25.01')), 'Social protection')
                .when(col("func_sub").isin("judiciary", "public safety") , "Public order and safety")
                # No classification into defence
                .when((
                    col('prog1').startswith('16') |
                    col('prog1').startswith('70') |
                    col('prog1').startswith('71') |
                    col('prog1').startswith('72') |
                    col('prog1').startswith('84') |
                    col('prog1').startswith('93') |
                    col('prog1').startswith('15')), 'Education')
                .when((
                    col('prog1').startswith('67') |
                    col('prog1').startswith('68') |
                    col('prog1').startswith('69') |
                    col('prog1').startswith('80')), 'Health')
                .when(col('prog1').startswith('4 ') | col('prog1').startswith('33') | col('prog1').startswith('73'), 'Recreation, culture and religion')
                .when(col('prog1').startswith('10'), 'Environmental protection') # 2021 and 2020 figures don't match
                .when(
                    ((
                        (col('prog1').startswith('54')) & (col('Water&SanitationTag') == False)
                    ) | (
                        (col('prog1').startswith('56') | col('prog1').startswith('91') | col('prog1').startswith('98')) & (col('Water&SanitationTag') == True)
                    )) ,'Housing and community amenities')

                # No Defence spending information
                .when((
                    (col('func_sub').isin('agriculture', 'transport')) |
                    (col('prog1').startswith('53') | col('prog1').startswith('26') | col('prog1').startswith('50') | col('prog1').startswith('51')) |
                    # energy spending
                    (col('activity').startswith('26') | col('prog1').startswith('52') | col('prog1').startswith('88') | col('prog1').startswith('89') | col('prog1').startswith('90'))                
                ), 'Economic affairs')
                .otherwise('General public services')
            )
            .withColumn('geo1', 
                        when(col('admin0')=='Central', 'Central Scope')
                        #.when(col('admin0')=='Regional', concat(col('admin1'), " District, Bhutan"))
                        )
        )
    
@dlt.table(name=f'btn_boost_gold')
def boost_gold():
    return (dlt.read(f'btn_boost_silver')
            .withColumn('country_name', lit('Bhutan')) 
            .select('country_name',
                    'adm1_name',
                    'year',
                    col('Budget').alias('approved'),
                    col('Executed').alias('executed'),
                    expr("CAST(NULL AS DOUBLE) as revised"),
                    'admin0',
                    'admin1',
                    'admin2',
                    'geo1',
                    'func',
                    'func_sub'
            )
    )



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

    
