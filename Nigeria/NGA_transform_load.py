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
    .select('*',
        substring('Region', 1, 2).alias("region_code_first2")
    )
    .join(dlt.read('nga_region_lookup'), on=["region_code_first2"], how="left")
    .withColumn('Year', col('Year').cast('int'))
    .withColumn(
        'is_transfer', col('Econ2').startswith('2207')
    ).filter(
        ~col('is_transfer')
    ).withColumn(
        'admin2', trim(regexp_replace(col("adm2"), '^[0-9\\s]*', ''))
    ).withColumn('func_sub',
        when(
            col("Func2").startswith('7033') , "judiciary" # Come before public safety tagging as it is a subcategory
        ).when(
            col('Func1').startswith('703'), 'public safety'
        )
    ).withColumn('func',
        when(
            (col("func_sub").isin("judiciary", "public safety")), "Public order and safety"
        ).when(
            col("Func1").startswith('705'), "Environmental protection"
        ).when(
            col("Func1").startswith('706'), "Housing and community amenities"
        ).when(
            col("Func1").startswith('704'), "Economic affairs"
        ).when(
            col("Func1").startswith('708'), "Recreation, culture and religion"
        ).when(
            (
                col('Func1').startswith('702') & (
                    ~(
                        col('Econ4').startswith('22010102') |
                        col('Econ4').startswith('22010104') |
                        col('Econ4').startswith('22021059')
                    )
                )
            ), 'Defense'
        ).when(
            (            
                col('Func1').startswith('709') & 
                    (
                        ~(
                            col('Econ4').startswith('22010102') |
                            col('Econ4').startswith('21030102') |
                            col('Econ4').startswith('22010104') |
                            col('Econ4').startswith('22021059')
                        )
                    ) &
                    (
                        col('Year').isin(2015, 2016)
                    )
            ), 'Education'
        ).when(
            col('Func1').startswith('709') & (~(col('Year').isin(2015, 2016))), 'Education'
        ).when(
            (            
                col('Func1').startswith('707') & 
                    (
                        ~(
                            col('Econ4').startswith('22010102') |
                            col('Econ4').startswith('22010104') |
                            col('Econ4').startswith('21030102') |
                            col('Econ4').startswith('22021059')
                        )
                    ) &
                    (
                        col('Year').isin(2015)
                    )
            ), 'Health'
        ).when(
            col('Func1').startswith('709') & ~col('Year').isin(2015, 2016), 'Education'
    ).when(
            (            
                col('Func1').startswith('701') & 
                    (
                        ~(
                            col('Econ4').startswith('22010102') |
                            col('Econ4').startswith('21030102') |
                            col('Econ4').startswith('22010104') |
                            col('Econ4').startswith('22021059')
                        )
                    ) 
            ), 'General public services'
        ).when(
            (
                col('Program').startswith('ERGP22112823') |
                col('adm3').startswith('161002') |
                col('Econ4').startswith('22040109') |
                col('Econ4').startswith('22021007') |
                col('Econ4').startswith('22010102') |
                col('Econ4').startswith('21030102') |
                col('Econ4').startswith('22010104') |
                col('Econ4').startswith('22021059')
            ), 'Social protection'
        )
    )
  )

@dlt.table(name='nga_boost_gold')
def nga_boost_gold():
  return(dlt.read('nga_boost_silver')
    .withColumn('country_name', lit(COUNTRY))
    .withColumn('admin0', lit("Central"))
    .withColumn('admin1', lit("Central Scope"))
    .select('country_name',
            col('Year').alias('year'),
            'admin0',
            'admin1',
            'admin2',
            'geo1',    
            'func',
            'func_sub',
            col('Approved').alias('approved'),
            col('Executed').alias('executed'),
    )    
  )
