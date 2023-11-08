# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# TODO: stack all country's microdata into 'boost_gold'
@dlt.table(name=f'boost_gold')
def boost_gold():
    return spark.table('boost_intermediate.moz_boost_gold')

@dlt.table(name=f'expenditure_by_country_year')
def expenditure_by_country_year():
    return (dlt.read(f'boost_gold')
        .groupBy("country_name", "year").agg(F.sum("executed").alias("Expenditure"))
    )
