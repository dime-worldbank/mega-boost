# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace, regexp_extract, substring, coalesce

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
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
            .withColumn('ECON1',coalesce(col('ECON1'), lit('')))
            .withColumn('Econ2',coalesce(col('Econ2'), lit('')))
            .withColumn('ADMIN1',coalesce(col('ADMIN1'), lit('')))
            .withColumn('ADMIN2',coalesce(col('ADMIN2'), lit('')))
            .withColumn('PROG', coalesce(col('PROG').cast('string'), lit('')))   
            .withColumn('Roads',coalesce(col('Roads'), lit('')))
            .withColumn('WSS',coalesce(col('WSS'), lit('')))
            .withColumn('railroads',coalesce(col('railroads'), lit('')))
            .withColumn('Maintenance',coalesce(col('Maintenance'), lit('')))
            .withColumn('subsidies',coalesce(col('subsidies'), lit('')))            
            .withColumn('Air',coalesce(col('Air'), lit('')))
            .withColumn('year', col('YEAR').cast('int'))        
        ).withColumn(
        'admin0_tmp', lit('Central')
        ).withColumn(
        'admin1_tmp', lit('Central Scope')
        ).withColumn(
        'admin2_tmp', trim(regexp_replace(col("ADMIN2"), '^[0-9\\s]*', ''))
        ).withColumn(
        'geo1', 
            when(col("GEO1").isNull(), "Central Scope")
            .when(col("GEO1").startswith("0") | col("GEO1").startswith("9"), "Central Scope")
            .when(col("GEO1").rlike('^[1-8]'), trim(regexp_replace(col("GEO1"), '^[1-8]+\\s*', ''))) 
        ).withColumn(
        'geo1', 
            when(col("geo1") == "BeBen Arous", "Ben Arous")
            .otherwise(col("geo1"))
        ).withColumn(
        'is_foreign', col('Econ2').startswith('09')
        ).withColumn(
        'func_sub',
            when((col('ADMIN1').startswith('06')) & (col('ADMIN2').startswith('07')), 'Public Safety')
            .when(col('ADMIN1').startswith('07'), 'Judiciary')
            .when(substring(col("ADMIN2"), 1, 2).isin('04 30 33'.split()), 'Tertiary Education')
            .when((col("ADMIN2").startswith("16") | col("ADMIN2").startswith("17")), 'Agriculture')
            .when(col('ADMIN1').startswith('18') , 'Telecom')
            .when(((col('Roads')==1) | (col('railroads') == 1) | (col('Air') == 1)), 'Transport')
        ).withColumn(
        'func',
            # housing
            when(col('WSS')==1, 'Housing and community amenities')
            # defence
            .when((col('ADMIN1').startswith('09') | col('ADMIN1').startswith('06')), 'Defence')
            # public order and safety
            .when(col("func_sub").isin('Public Safety', 'Judiciary') , "Public order and safety")
            # environment protection
            .when(col('ADMIN2').startswith('21'), 'Environmental protection')
            # health
            .when(col('ADMIN2').startswith('27') | col('ADMIN2').startswith('34'), 'Health')
            # social protection
            .when(col('ADMIN1').startswith('05'), 'Social protection')
            # education
            .when(substring(col("ADMIN2"), 1, 2).isin('04 29 30 33 37 39 40'.split()), 'Education')

            # recreation, culture and religion
            .when(substring(col("ADMIN1"), 1, 2).isin('19 10 20'.split()), 'Recreation, culture and religion')
            # economic affairs
            .when(col("func_sub").isin('Agriculture', 'Transport', 'Telecom') , "Economic affairs")
            # general public services
            .otherwise('General public services')
        ).withColumn(
        'econ_sub',
            when((col('YEAR')>2015) & (col('PROG') == '2 Securite Sociale'), 'Pensions') # appears before Social Assistance. Available post 2015
            .when((col('ADMIN1').startswith('05') & (col('PROG')!='2 Securite Sociale')), 'Social Assistance')
            .when((col('Econ2').startswith('01') & (col('PROG')!='2 Securite Sociale')), 'Basic Wages')
            .when(((col('Maintenance') == 1) & col('ECON1').startswith('Titre 2')), 'Capital Maintenance')
            .when(((col('Maintenance') == 1) & col('ECON1').startswith('Titre 1')), 'Recurrent Maintenance')
            .when(((col('subsidies')==1) & (~col('ECON2').startswith('02')) &
                   (~col('ECON2').startswith('01'))), 'Subsidies to Production')
        ).withColumn(
        'econ',
            # wage bill
            when((col('Econ2').startswith('01') & (col('PROG')!='2 Securite Sociale')), 'Wage bill')
            # cap ex
            .when((col('ECON1').startswith('Titre 2') & (~(col('Econ2').startswith('10')))), 'Capital expenditures')
            # goods and services
            .when((col('Econ2').startswith('02') & (col('PROG')!='2 Securite Sociale')), 'Goods and services')
            # subsidies
            .when(((col('subsidies')==1) & (~col('Econ2').startswith('02')) & (~col('Econ2').startswith('01'))), 'Subsidies')
            # social benefits
            .when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits')
            # interest on debt
            .when(col('Econ2').startswith('05'), 'Interest on debt')
            # other expenses
            .otherwise('Other expenses')
        )

@dlt.table(name=f'tun_boost_gold')
def boost_gold():
    return (dlt.read(f'tun_boost_silver')
            .filter(~col('ECON2').startswith('10')) # debt repayment
            .withColumn('country_name', lit('Tunisia')) 
            .select('country_name',
                    'year',
                    col('OUVERT').alias('approved'),
                    col('ORDONNANCE').alias('revised'),
                    col('PAYE').alias('executed'),
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
