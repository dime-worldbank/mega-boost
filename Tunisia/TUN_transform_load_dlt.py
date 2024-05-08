# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace, regexp_extract, substring, coalesce

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/Tunisia'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'tun_boost_bronze')
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

@dlt.table(name=f'tun_boost_silver')
def boost_silver():
    return (dlt.read(f'tun_boost_bronze')
            .withColumn('adm1_name', 
                when(col("GEO1").isNull(), "Central Scope")
                .when(col("GEO1").startswith("0") | col("GEO1").startswith("9"), "Other")
                .when(col("GEO1").rlike('^[1-8]'), trim(regexp_replace(col("GEO1"), '^[1-8]+\\s*', '')))
                )
        ).withColumn(
        'admin0_tmp', lit('Central')
        ).withColumn(
        'admin1_tmp', lit('Central')
        ).withColumn(
        'admin2_tmp', trim(regexp_replace(col("ADMIN2"), '^[0-9\\s]*', ''))
        ).withColumn(
        'geo1', 
            when(col("GEO1").isNull(), "Central Scope")
            .when(col("GEO1").startswith("0") | col("GEO1").startswith("9"), "Central Scope")
            .when(col("GEO1").rlike('^[1-8]'), trim(regexp_replace(col("GEO1"), '^[1-8]+\\s*', ''))) 
        ).withColumn(
        'is_foreign', col('Econ2').startswith('09')
        ).withColumn(
        'func_sub',
            when((col('ADMIN1').startswith('06')) & (col('ADMIN2').startswith('07')), 'public safety')
            .when(col('ADMIN1').startswith('07'), 'judiciary')
            .when(substring(col("ADMIN2"), 1, 2).isin('04 30 33'.split()), 'tertiary education')
            .when((col("ADMIN2").startswith("16") | col("ADMIN2").startswith("17")), 'agriculture')
            .when(col('ADMIN1').startswith('18') , 'telecom')
            .when(((coalesce(col('Roads'), lit('')) ==1) | (coalesce(col('railroads'), lit('')) == 1) | (coalesce(col('Air'), lit('')) == 1) | (coalesce(col('WSS'), lit(''))==1)), 'transport')
        ).withColumn(
        'func',
            when((col('ADMIN1').startswith('09') | col('ADMIN1').startswith('06')), 'Defence')
            .when(col("func_sub").isin('public safety', 'judiciary') , "Public order and safety")
            .when(col('ADMIN2').startswith('21'), 'Environmental protection')
            .when(col('ADMIN2').startswith('27') | col('ADMIN2').startswith('34'), 'Health')
            .when(col('ADMIN1').startswith('05'), 'Social protection')
            .when(substring(col("ADMIN2"), 1, 2).isin('04 29 30 33 37 39 40'.split()), 'Education')
            .when(coalesce(col('WSS'), lit(''))==1, 'Housing and community amenities')
            .when(substring(col("ADMIN1"), 1, 2).isin('19 10 20'.split()), 'Recreation, culture and religion')
            .when(col("func_sub").isin('agriculture', 'transport', 'telecom') , "Economic affairs")
            .otherwise('General public services')
        ).withColumn(
        'econ_sub',
            when(col('ECON2').startswith('01'), 'basic wages')
            .when(((coalesce(col('Maintenance'), lit('')) == 1) & col('ECON1').startswith('Titre 2')), 'capital maintenance')
            .when(((coalesce(col('Maintenance'), lit('')) == 1) & col('ECON1').startswith('Titre 1')), 'recurrent maintenance')
            .when(coalesce(col('subsidies'), lit(''))==1, 'subsidies to production')
            .when((col('YEAR')>2015) & (col('PROG') == '2 Securite Sociale'), 'pensions') # appears before social assistance. Available post 2015
            .when((col('ADMIN1').startswith('05') & (coalesce(col('PROG'), lit(''))!='2 Securite Sociale')), 'social assistance')

        ).withColumn(
        'econ',
            when(col('ECON2').startswith('01'), 'Wage bill')
            .when((col('ECON1').startswith('Titre 2') & (~(coalesce(col('ECON2'), lit('')).startswith('10')))), 'Capital expenditure')
            .when(col('ECON2').startswith('02'), 'Goods and services')
            .when(coalesce(col('subsidies'), lit(0))==1, 'Subsidies')
            .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
            .otherwise('Other expenses')
        )

@dlt.table(name=f'tun_boost_gold')
def boost_gold():
    return (dlt.read(f'tun_boost_silver')
            .filter(~((col('Maintenance')==1)& (col('ECON1').startswith('Titre 2'))))
            .withColumn('country_name', lit('Tunisia')) 
            .select('country_name',
                    'adm1_name',
                    col('YEAR').alias('year'),
                    col('OUVERT').alias('approved'),
                    col('ORDONNANCE').alias('revised'),
                    col('PAYE').alias('executed').cast('int'),
                    col('admin0_tmp').alias('admin0'),
                    col('admin1_tmp').alias('admin1'),
                    col('admin2_tmp').alias('admin2'),
                    'geo1',
                    'is_foreign',
                    'func',
                    'func_sub',
                    'econ',
                    'econ_sub'
                    )
    )
