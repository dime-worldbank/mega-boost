# Databricks notebook source
import dlt
import json
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_extract, regexp_replace, when, lit, substring, expr, floor, concat, udf
from pyspark.sql.types import StringType


# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Albania'
RAW_COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'
RAW_INPUT_DIR = f"{TOP_DIR}/Documents/input/Data from authorities/"

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

with open(f"/dbfs/{RAW_INPUT_DIR}/{COUNTRY}/2023/labels_en_v01_overall.json", 'r') as json_file:
    labels = json.load(json_file)

def replacement_udf(column_name):
    def replace_value(value):
        if value is None:
            return value
        value_str = str(value).split('.')[0]
        return labels.get(column_name, {}).get(value_str, value_str)
    return udf(replace_value, StringType())

@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'alb_2023_onward_boost_bronze')
def boost_2023_onward_bronze():
    file_paths = [f"{RAW_COUNTRY_MICRODATA_DIR}/2023.csv"]
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(file_paths))
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df

@dlt.table(name=f'alb_2023_onward_boost_silver')
def boost_silver():
    silver_df  = (dlt.read(f'alb_2023_onward_boost_bronze')
        ).filter(col("approved").isNull()
        ).filter(~lower(col("project").substr(1, 5)).contains("total")
        ).withColumn("admin1", substring(col("admin4").cast("string"), 1, 1)
        ).withColumn("admin3", 
            when((col("admin1") == "2") & (col("admin3") == 0), 100)
            .otherwise(col("admin3"))
        # econ tagging
        ).withColumn("econ1", floor(col("econ5") / 1000000)
        ).withColumn("econ2", floor(col("econ5") / 100000)
        ).withColumn("econ4", floor(col("econ5") / 1000)
        ).withColumn("econ3", when(col("econ3") == 6780, 678).otherwise(col("econ3"))
        ).withColumn("econ3_str", col("econ3").cast("string")
        ).withColumn(
            "econ1", 
            when(col("econ1").isNull(), substring(col("econ3_str"), 1, 1).cast("int"))
            .otherwise(col("econ1"))
        ).withColumn(
            "econ2",
            when(col("econ2").isNull(), substring(col("econ3_str"), 1, 2).cast("int"))
            .otherwise(col("econ2"))
        ).drop("econ3_str"
        ).filter((col("econ3") != 255) & (col("econ3") >= 230)
        ).filter((col("econ1") != 17) & (col("econ1") != 16)
        ).withColumn("econ1", when(col("executed").isNull(), floor(col("econ3") / 100))
            .otherwise(col("econ1"))
        ).withColumn("econ2", when(col("executed").isNull(), floor(col("econ3") / 10))
            .otherwise(col("econ2"))
        # functional tagging
        ).withColumn("program1", col("func3")
        ).withColumn("func3", col("func3").cast("double")
        ).withColumn("func3_n", 
            when((col("year") == 2023) & col("func3").isNull() & (col("program1") != ""),
                substring(col("project"), 2, 3))
            .otherwise(lit(None))
        ).withColumn("func3_n", when(col("func3_n").isNotNull(), concat(col("func3_n"), lit("0")))
            .otherwise(col("func3_n"))
        ).withColumn("func3_n", when(col("func3_n").isin(["0140", "0430", "0660", "0910"]),
                concat(lit("1"), col("func3_n")))
            .otherwise(col("func3_n"))
        ).withColumn("func3_n", col("func3_n").cast("double")
        ).withColumn("func3", when((col("year") == 2023) & col("func3").isNull() & (col("program1") != ""),
                    col("func3_n"))
            .otherwise(col("func3"))
        ).withColumn("func1", (col("func3") / 1000).cast("int")
        ).withColumn("func2", (col("func3") / 100).cast("int")
        ).withColumnRenamed("func3", "program"
        # expense type
        ).withColumn("exp_type", lit(None).cast("integer")
        ).withColumn("exp_type", when(col("econ3").isin([600, 601]), 1).otherwise(col("exp_type"))
        ).withColumn("exp_type", 
            when(col("econ2").isin([65, 66]) | col("econ3").isin([602, 603, 604, 605, 606]), 2)
            .otherwise(col("exp_type"))
        ).withColumn("exp_type", when(col("econ1") == 2, 3).otherwise(col("exp_type"))
        ).withColumn("exp_type", 
            when((col("econ2") == 67) | (col("econ3") == 609), 4)
            .otherwise(col("exp_type"))
        # project source
        ).withColumn("project_source", substring(col("project"), 1, 1)
        ).withColumn("project_s", 
            when(col("project_source") == "0", 1)
            .when(col("project_source") == "A", 2)
            .when(col("project_source") == "G", 3)
            .when(col("project_source") == "K", 4)
            .when(col("project_source") == "M", 5)
            .otherwise(None)
        ).withColumn(
            "project_s", when(col("project_s").isNull() & (col("project_source") != "."), 1)
            .otherwise(col("project_s"))
        ).drop("project_source"
        # transfers
        ).withColumn("transfer", when((col("econ5") == 6040005) & (col("admin2") == 1), 1)
            .when(col("econ5").isin([6040002, 6040001, 6041100, 6040006, 6040007]), 1)
            .when((col("econ3") == 604) & (col("admin4") == 1010098), 1)
            .when((col("econ3") == 604) & (col("admin4") == 1025096) & (col("admin3") == 25), 1)
            .when(col("econ5") == 6040010, 1)
            .when((col("econ3") == 604) & (col("admin4") == 1010226) & (col("admin3") == 10), 1)
            .otherwise(lit(0))
        )
    for column_name, mapping in labels.items():
        if column_name in silver_df.columns:
            silver_df = silver_df.withColumn(column_name, replacement_udf(column_name)(col(column_name)))
    return silver_df.filter(col('transfer')=='Excluding transfers')
