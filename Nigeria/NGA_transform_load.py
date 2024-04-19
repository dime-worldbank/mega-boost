# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, expr, trim, regexp_replace

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Nigeria'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'nga_boost_bronze')
def nga_boost_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{COUNTRY_MICRODATA_DIR}/central.csv')
    )

@dlt.table(name=f'nga_region_lookup')
def nga_region_lookup():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(f'{INPUT_DIR}/../Auxiliary/NigeriaRegionLookup.csv')
    )

# currently NGA only has central data which is all geo tagged
@dlt.expect_or_fail("geo1_not_null", "geo1 IS NOT NULL")
@dlt.table(name=f'nga_boost_silver')
def nga_boost_silver():
  return (dlt.read('nga_boost_bronze')
    .select('*', substring('Region', 1, 2).alias("region_code_first2"))
    .join(dlt.read('nga_region_lookup'), on=["region_code_first2"], how="left")
    .withColumn('Year', col('Year').cast('int'))
    .withColumn('is_transfer', col('Econ2').startswith('2207'))
    .withColumn('admin2', trim(regexp_replace(col("adm2"), '^[0-9\\s]*', '')))
    .withColumn('func_sub',
        when(col("Func2").startswith('7033') , "judiciary") # Comes before public safety tagging as it is a subcategory
        .when(col('Func1').startswith('703'), 'public safety')
        # No breakdown of spending into primary, secondary for health
        # No breakdown of spending into primary, secondary education
    ).withColumn('func',
        # Public order and safety
        when((col("func_sub").isin("judiciary", "public safety")), "Public order and safety")
        # Environmental protection
        .when(col("Func1").startswith('705'), "Environmental protection")
        # Housing and community amenities
        .when(col("Func1").startswith('706'), "Housing and community amenities")
        # Econ affairs
        .when(col("Func1").startswith('704'), "Economic affairs")
        # Recreation and culture
        .when(col("Func1").startswith('708'), "Recreation, culture and religion")
        # Defense
        .when(((~col('is_transfer')) &col('Func1').startswith('702') & 
                (~(col('Econ4').startswith('22010102') |
                   col('Econ4').startswith('22010104') |
                   col('Econ4').startswith('21030102') |
                   col('Econ4').startswith('22021059')))
            ), 'Defense')
        # Education
        .when((col('Func1').startswith('709') &
                (~(col('Econ4').startswith('22010102') |
                   col('Econ4').startswith('21030102') |
                   col('Econ4').startswith('22010104') |
                   col('Econ4').startswith('22021059'))) &
                (col('Year').isin(2015, 2016))
                ), 'Education')
        .when(col('Func1').startswith('709') & (~(col('Year').isin(2015, 2016))), 'Education')
        # Health
        .when(((~col('is_transfer')) & col('Func1').startswith('707') & 
                (~(col('Econ4').startswith('22010102') |
                   col('Econ4').startswith('22010104') |
                   col('Econ4').startswith('21030102') |
                   col('Econ4').startswith('22021059'))) &
                (col('Year').isin(2015))), 'Health')
        .when(((col('Year')>2015) & (~col('is_transfer')) & (col('Func1').startswith('707'))), 'Health')
        # social protection 
        # (No data)
        # general public services
        .otherwise('General public services')
    ).withColumn('econ_sub',
        when((col('Econ2').startswith('2101')), 'basic wages') # redundant condition in excel with two conditions on Econ2
        .when(((~col('is_transfer')) & (col('Econ3').startswith('210201'))), 'allowances')
        .when(((~col('is_transfer')) & (col('Econ4').startswith('21020202'))), 'social benefits')
        .when((
            (~col('is_transfer')) &
            (col('Econ1').startswith('23')) &
            (col('Econ3').startswith('220402'))), 'capital expenditure (foreign spending)') # no entries in the excel sheet for this entry, but formula is present
        .when(col('Econ2').startswith('2303'), 'capital maintenance')
        .when(((~col('is_transfer')) & (col('Econ3').startswith('220202'))), 'basic services')
        .when(((~col('is_transfer')) & (col('Econ3').startswith('220207'))), 'employment contracts')
        .when(((~col('is_transfer')) & (col('Econ3').startswith('220204'))), 'recurrent maintenance')
        .when(((~col('is_transfer')) & (col('Econ2').startswith('2205'))), 'subsidies to production')
        .when((~col('is_transfer')) & (
            (col('Econ4').startswith('22040109') | col('Econ4').startswith('22021007')) |
            (col('Program').startswith('ERGP22112823')) |
            (col('adm3').startswith('161002'))
            ), 'social assistance')
        .when(((~col('is_transfer')) &
                (col('Econ4').startswith('22010102') |
                col('Econ4').startswith('21030102') |
                col('Econ4').startswith('22010104') |
                col('Econ4').startswith('22021059'))), 'pensions')
    ).withColumn('source',
                 when(((~col('is_transfer')) & (col('Econ3').startswith('220402'))), 'foriegn'))
    .withColumn('econ',
        # wage bill
        when(((col('Econ1').startswith('21')) & (~col('Econ2').startswith('2103')) & (~col('Econ4').startswith('21030102'))), 'Wage bill')
        # capital expenditure
        .when(((~col('is_transfer')) & 
               (col('Econ1').startswith('23')) &
               (~col('Econ4').startswith('22040109')) &
               (~col('Econ4').startswith('22021007')) &
               (~col('Program').startswith('ERGP22112823'))), 'Capital expenditure')
        # goods and services
        .when(((~col('is_transfer')) &
               (col('Econ1').startswith('22')) &
               (~col('Econ2').startswith('2201')) &
               (~col('Econ2').startswith('2205')) &
               (~col('Econ4').startswith('22021059')) &
               (~col('Program').startswith('ERGP22112823')) &
               (~col('Econ4').startswith('22040109')) &
               (~col('Econ4').startswith('22021007'))), 'Goods and services')
        # subsidies
        .when(col('Econ2').startswith('2205'), 'Subsidies')
        .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
        .otherwise('Other expenses') # No formula available for 'Other grants and services'
    )
)
    
@dlt.table(name='nga_boost_gold')
def nga_boost_gold():
  return(dlt.read('nga_boost_silver')
    .filter((~col('is_transfer')) & (~col('Econ1').startswith('41')))
    .withColumn('country_name', lit(COUNTRY))
    .withColumn('admin0', lit("Central"))
    .withColumn('admin1', lit("Central Scope"))
    .select('country_name',
            col('Year').alias('year'),
            'admin0',
            'admin1',
            'admin2',
            'geo1',
            col('Approved').alias('approved'),
            col('Executed').alias('executed'),    
            'func',
            'func_sub',
            'econ_sub',
            'econ',
            'source'
    )    
  )
