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
        .agg(F.min("year").alias("year"))
    )
    base_cpis = (earliest_years.join(spark.table('indicator.consumer_price_index'), on=["country_name", "year"], how="inner")
        .select(F.col('year').alias('base_cpi_year'),
                'country_name',
                F.col('cpi').alias('base_cpi'))
    )
    return (spark.table('indicator.consumer_price_index')
        .join(base_cpis, on=["country_name"], how="inner")
        .withColumn("cpi_factor", F.col("cpi") / F.col('base_cpi'))
        .select('country_name', "year", "cpi_factor")
    )

@dlt.table(name=f'expenditure_by_country_year')
def expenditure_by_country_year():
    cpi_factors = dlt.read('cpi_factor')
    return (dlt.read(f'boost_gold')
        .groupBy("country_name", "year").agg(F.sum("executed").alias("expenditure"))
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
    )

@dlt.table(name=f'expenditure_by_country_adm1_year')
def expenditure_by_country_adm1_year():
    cpi_factors = dlt.read('cpi_factor')
    return (dlt.read(f'boost_gold')
        .groupBy("country_name", "adm1_name", "adm1_name_alt", "year").agg(F.sum("executed").alias("expenditure"))
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
    )
