# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, expr
from pyspark.sql.types import StringType


# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Bangladesh'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'bgd_boost_bronze')
def boost_bronze():
    # TODO: expand to 2018 and before, which have diff columns
    file_paths = [f"{COUNTRY_MICRODATA_DIR}/2019.csv", f"{COUNTRY_MICRODATA_DIR}/202*.csv"]

    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(file_paths))
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df

@dlt.table(name=f'bgd_boost_silver')
def boost_silver():
    return (dlt.read(f'bgd_boost_bronze')
        .filter(col('ECON1').isin(['3 - Recurrent Expenditure', '4 - Capital Expenditure']))
        .withColumn(
            'geo1', 
            regexp_replace(col("GEO1"), r"\d+ ", "") # remove leading numbers
        ).withColumn(
            'admin0', 
            when((col('ADMIN0').endswith('CAFO') | col('ADMIN0').endswith('DAFO')), 'Regional')
            .otherwise('Central')
        ).withColumn(
            'admin1',
            when(col('admin0') == 'Central', 'Central')
            .otherwise(col('geo1'))
        ).withColumn(
            'admin2',
            col('ADMIN2')
        ).withColumn(
            'func',
            when(col('FUNC2') == 'Defence Services', 'Defence')
            .when(col('FUNC2') == 'Public Order and Safety', 'Public order and safety')
            .when(col('FUNC2') == 'Housing', 'Housing and community amenities')
            .when(col('FUNC2') == 'Health', 'Health')
            .when(col('FUNC2') == 'Recreation, Culture and Religious Affairs', 'Recreation, culture and religion')
            .when(col('FUNC2') == 'Education and Technology', 'Education')
            .when(col('FUNC2') == 'Social Security and Welfare', 'Social protection')
            .when(
                col('ADMIN2').isin([
                    "145-Ministry of Environment, Forest and Climate Change",
                    "14503-Department of Environment"
                ]),
                'Environmental protection')
            .when(
                ((col('FUNC1') == 'Economic Sector')
                & (~col('FUNC2').isin(['Housing', 'Local Government and Rural Development']))),
                'Economic affairs')
            .otherwise(lit('General public services'))
        )
        .withColumn(
            'func_sub',
            when(col('func') == 'Public order and safety', 
                 when(col('ADMIN2') == '122-Public Security Division', 'public order')
                 .otherwise('judiciary')
            )
            .when(col('func') == 'Education', 
                when(
                    (col('ADMIN2').contains('Primary Education') 
                    | col('ADMIN2').contains('Primary and Mass Education'))
                , 'primary education')
                .when(
                    (col('ADMIN2').contains('Secondary and Higher Education') 
                    | col('ADMIN2').contains('Secondary & Higher Education'))
                , 'secondary education')
            )
        ).withColumn(
            'econ_sub',
            when(col('ECON3') == "311 - Wages and Salaries", 
                when(col('ECON_5') == '31113 - Allowances', 'allowances')
                .otherwise('basic wages')
            )
            .when(
                (col('Old_ECON3').startswith("3211113")
                 | col('Old_ECON3').startswith("3211114")
                 | col('Old_ECON3').startswith("3211115")
                 | col('Old_ECON3').startswith("3211120")
                 | col('Old_ECON3').startswith("3211122")
                 | col('Old_ECON3').startswith("3211129")
                ), 'basic services')
            .when(
                col('ECON_4') == '3257 - Professional services, honorariums and special expenses',
                'employment contracts')
            .when((col('ECON3').startswith('372')), 'social assistance')
            .when(col('ECON_5') == '37311 - Employment-related social benefits in cash', 'pensions')
        ).withColumn(
            'econ',
            when(col('ECON1') == '4 - Capital Expenditure', 'Capital expenditures')
            .when(col('ECON2') == '31 - Compensation of Employees', 'Wage bill')
            .when(col('ECON2') == '32 - Purchases of Goods and Services', 'Goods and services')
            .when(col('ECON2') == '34 - Interest', 'Interest on debt') 
            .when(col('ECON2') == '35 - Subsidies', 'Subsidies')
            .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
            .otherwise('Other expenses') 
        )
    )

@dlt.table(name=f'bgd_boost_gold')
def boost_gold():
    return (dlt.read(f'bgd_boost_silver')
            .withColumn('country_name', lit(COUNTRY))
            .select('country_name',
                    'year',
                    col('BUDGET').alias('approved'),
                    col('REVISED').alias('revised'),
                    col('EXECUTED').alias('executed'),
                    'admin0',
                    'admin1',
                    'admin2',
                    'geo1',
                    'func',
                    'func_sub',
                    'econ',
                    'econ_sub'
        )
    )
