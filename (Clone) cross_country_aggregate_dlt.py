# Databricks notebook source
import dlt
from pyspark.sql import functions as F

quality_source_schema = spark.conf.get("QUALITY_SOURCE_SCHEMA", 'boost_intermediate')
country_source_schema = spark.conf.get("COUNTRY_SOURCE_SCHEMA", 'boost_intermediate')

catalog = 'prd_mega'


# COMMAND ----------

# Adding a new country requires adding the country here
country_codes = ["tun"]

@dlt.table(
    name='boost_dev',
)
def boost_dev():
    unioned_df = None
    for code in country_codes:
        table_name = f"{catalog}.{country_source_schema}.{code}_boost_gold_test"
        current_df = spark.table(table_name)
        current_df = (current_df
            .withColumn("adm1_name", F.col("geo1"))
            .withColumn("geo0",
                F.when((F.col("geo1") == "Central Scope") | (F.col("geo1").isNull()), F.lit("Central"))
                .otherwise(F.lit("Regional"))
            )
        )

        col_order = [
            "index",
            "country_name",
            "year",
            "admin0",
            "admin1",
            "admin2",
            "geo0",
            "geo1",
            "adm1_name",
            "func",
            "funcsub",
            "econ",
            "econsub",
            "is_foreign",
            "approved",
            "revised",
            "executed",
        ]
        current_df = current_df.select(col_order)

        if unioned_df is None:
            unioned_df = current_df
        else:
            unioned_df = unioned_df.union(current_df)
    return unioned_df

# COMMAND ----------

@dlt.table(
    name="expenditure_by_country_year_dev",
)
def expenditure_by_country_year_dev():
    quality_total_dev = spark.table(
        f"{catalog}.{quality_source_schema}.quality_total_gold"
    )
    boost_gold_dev = dlt.read("boost_dev")
    boost_gold_dev = (
        boost_gold_dev
        .dropDuplicates(subset=["country_name", "index"])
        .groupBy("country_name", "year")
        .agg(
            F.sum("executed").alias("expenditure"),
            F.sum("approved").alias("budget"),
        )
    )
    return (
        boost_gold_dev.join(quality_total_dev, on=["country_name", "year"], how="inner")
        .withColumn(
            "diff_expenditure",
            (F.col("executed") - F.col("expenditure")).cast("integer"),
        )
        .withColumn(
            "diff_budget", (F.col("approved") - F.col("budget")).cast("integer")
        )
    )

# COMMAND ----------

@dlt.table(
    name="expenditure_by_country_year_econ_dev",
)
def expenditure_by_country_year_econ_dev():
    quality_econ_dev = spark.table(
        f"{catalog}.{quality_source_schema}.quality_economic_gold"
    )
    boost_gold_dev = dlt.read("boost_dev")
    boost_gold_dev = (
        boost_gold_dev
        .dropDuplicates(subset=["country_name", "econ", "index"])
        .groupBy("country_name", "econ", "year")
        .agg(
            F.sum("executed").alias("expenditure"),
            F.sum("approved").alias("budget"),
        )
    )
    return (
        boost_gold_dev.join(quality_econ_dev, on=["country_name", "econ", "year"], how="inner")
        .withColumn(
            "diff_expenditure",
            (F.col("executed") - F.col("expenditure")).cast("integer"),
        )
        .withColumn(
            "diff_budget", (F.col("approved") - F.col("budget")).cast("integer")
        )
    )

# COMMAND ----------

@dlt.table(
    name="expenditure_by_country_year_econ_sub_dev",
)
def expenditure_by_country_year_econ_sub_dev():
    quality_econ_sub = spark.table(
        f"{catalog}.{quality_source_schema}.quality_economic_sub_gold"
    )
    boost_gold_dev = dlt.read("boost_dev")
    boost_gold_dev = boost_gold_dev.withColumnRenamed("econsub", "econ_sub")
    boost_gold_dev = (
        boost_gold_dev
        .dropDuplicates(subset=["country_name", "econ_sub", "index"])
        .groupBy("country_name", "econ_sub", "year")
        .agg(
            F.sum("executed").alias("expenditure"),
            F.sum("approved").alias("budget"),
        )
    )
    return (
        boost_gold_dev.join(quality_econ_sub, on=["country_name", "econ_sub", "year"], how="inner")
        .withColumn(
            "diff_expenditure",
            (F.col("executed") - F.col("expenditure")).cast("integer"),
        )
        .withColumn(
            "diff_budget", (F.col("approved") - F.col("budget")).cast("integer")
        )
    )

# COMMAND ----------

@dlt.table(
    name="expenditure_by_country_year_func_dev",
)
def expenditure_by_country_year_func_dev():
    quality_func_dev = spark.table(
        f"{catalog}.{quality_source_schema}.quality_functional_gold"
    )
    boost_gold_dev = dlt.read("boost_dev")
    boost_gold_dev = (
        boost_gold_dev
        .dropDuplicates(subset=["country_name", "func", "index"])
        .groupBy("country_name", "func", "year")
        .agg(
            F.sum("executed").alias("expenditure"),
            F.sum("approved").alias("budget"),
        )
    )
    return (
        boost_gold_dev.join(quality_func_dev, on=["country_name", "func", "year"], how="inner")
        .withColumn(
            "diff_expenditure",
            (F.col("executed") - F.col("expenditure")).cast("integer"),
        )
        .withColumn(
            "diff_budget", (F.col("approved") - F.col("budget")).cast("integer")
        )
    )

# COMMAND ----------

@dlt.table(
    name="expenditure_by_country_year_func_sub_dev",
)
def expenditure_by_country_year_func_sub_dev():
    quality_func_sub = spark.table(
        f"{catalog}.{quality_source_schema}.quality_functional_sub_gold"
    )
    boost_gold_dev = dlt.read("boost_dev")
    boost_gold_dev = boost_gold_dev.withColumnRenamed("funcsub", "func_sub")
    boost_gold_dev = (
        boost_gold_dev
        .dropDuplicates(subset=["country_name", "func_sub", "index"])
        .groupBy("country_name", "func_sub", "year")
        .agg(
            F.sum("executed").alias("expenditure"),
            F.sum("approved").alias("budget"),
        )
    )
    return (
        boost_gold_dev.join(quality_func_sub, on=["country_name", "func_sub", "year"], how="inner")
        .withColumn(
            "diff_expenditure",
            (F.col("executed") - F.col("expenditure")).cast("integer"),
        )
        .withColumn(
            "diff_budget", (F.col("approved") - F.col("budget")).cast("integer")
        )
    )
