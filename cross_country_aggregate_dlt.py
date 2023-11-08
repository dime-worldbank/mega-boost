# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit
from pyspark.sql.window import Window

# TODO: stack all country's microdata into 'boost_gold'
@dlt.table(name=f'boost_gold')
def boost_gold():
    return (dlt.read(f'boost_intermediate.moz_boost_gold')
        .select('*')
    )

@dlt.table(name=f'expenditure_by_country_year')
def expenditure_by_country_year():
    country_year_part = Window.partitionBy("country_name", "year")
    return (dlt.read(f'boost_gold')
        .withColumn('Expenditure', sum(col("executed")).over(country_year_part))
    )

