# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, StringType

# BOOST source URL per country.
# To update a URL, edit the source_url value for the relevant country below.
SOURCE_URLS = [
    {
        "country_name": "Albania",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038087/Albania-BOOST-platform",
    },
    {
        "country_name": "Armenia",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0040229/Armenia-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Kiribati",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0042010/Kiribati-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Burundi",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0040668/Burundi-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Seychelles",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038067/Seychelles-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Haiti",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038072/Haiti-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Peru",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0043632/Peru-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Mauritania",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038077/Mauritania-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Kenya",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038086/Kenya-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Uganda",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038076/Uganda-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Mexico",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038091/Mexico-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Brazil",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0040769/Brazil-BOOST-Public-Expenditure-Database-",
    },
    {
        "country_name": "Uruguay",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038074/Uruguay-BOOST-public-expenditure-database",
    },
    {
        "country_name": "Niger",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038073/Niger-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Moldova",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038070/Moldova-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Guatemala",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038078/Guatemala-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Poland",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038071/Poland-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Tunisia",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038082/Tunisia-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Paraguay",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038079/Paraguay-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Burkina Faso",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0041709/Burkina-Faso-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Togo",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0040663/Togo-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Mali",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0042337/Mali-BOOST-Public-Expenditure-Database",
    },
    {
        "country_name": "Solomon Islands",
        "source_url": "https://datacatalog.worldbank.org/int/search/dataset/0038075/Solomon-Islands-BOOST-Public-Expenditure-Database",
    },
]

# COMMAND ----------

source_table_name = 'prd_mega.boost.source_urls'
schema = StructType(
    [
        StructField("country_name", StringType()),
        StructField("source_url", StringType()),
    ]
)
df = spark.createDataFrame(SOURCE_URLS, schema=schema)
df.write.mode("overwrite").saveAsTable(source_table_name)
