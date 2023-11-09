# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# TODO: stack all country's microdata into 'boost_gold'
@dlt.table(name=f'boost_gold')
def boost_gold():
    return spark.table('boost_intermediate.moz_boost_gold')

@dlt.table(name=f'cpi_factor')
def cpi_factor():
    earliest_years = (dlt.read('boost_gold')
        .groupBy("country_name")
        .agg(F.min("year").alias("earliest_year"))
    )
    cpi_data = (dlt.read('indicator.consumer_price_index')
        .join(earliest_years, on=["country_name", "year"], how="inner")
        .withColumn("cpi_factor", F.col("cpi") / F.first("cpi").over(Window.partitionBy("country").orderBy("year")))
        .select("country", "year", "cpi_factor")
    )
    return cpi_data

@dlt.table(name=f'expenditure_by_country_year')
def expenditure_by_country_year():
    cpi_factors = dlt.read('cpi_factor')
    return (dlt.read(f'boost_gold')
        .groupBy("country_name", "year").agg(F.sum("executed").alias("expenditure"))
        .join(cpi_factors, on=["country", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") * F.col("cpi_factor"))
    )
