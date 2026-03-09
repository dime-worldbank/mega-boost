# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, StringType

# BOOST source URL per country.
# To update a URL, edit the source_url value for the relevant country below.
SOURCE_URLS = [
    {"country_name": "Albania", "source_url": ""},
    {"country_name": "Bangladesh", "source_url": ""},
    {"country_name": "Bhutan", "source_url": ""},
    {"country_name": "Burkina Faso", "source_url": ""},
    {"country_name": "Chile", "source_url": ""},
    {"country_name": "Colombia", "source_url": ""},
    {"country_name": "Congo DR", "source_url": ""},
    {"country_name": "Ghana", "source_url": ""},
    {"country_name": "Kenya", "source_url": ""},
    {"country_name": "Liberia", "source_url": ""},
    {"country_name": "Mozambique", "source_url": ""},
    {"country_name": "Nigeria", "source_url": ""},
    {"country_name": "Pakistan", "source_url": ""},
    {"country_name": "Paraguay", "source_url": ""},
    {"country_name": "South Africa", "source_url": ""},
    {"country_name": "Togo", "source_url": ""},
    {"country_name": "Tunisia", "source_url": ""},
    {"country_name": "Uruguay", "source_url": ""},
]

# COMMAND ----------

@dlt.table(name='source_urls_bronze')
def source_urls_bronze():
    schema = StructType([
        StructField("country_name", StringType()),
        StructField("source_url", StringType()),
    ])
    return spark.createDataFrame(SOURCE_URLS, schema=schema)
