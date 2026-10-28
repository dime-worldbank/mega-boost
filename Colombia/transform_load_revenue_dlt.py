# Databricks notebook source
import dlt
from pyspark.sql.functions import col, lit, when, expr, sum, abs as spark_abs

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Colombia'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
# Matches the extract's root check
ROOT_RELATIVE_TOLERANCE = 1e-4

GOLD_COLUMNS = ['country_name', 'year', 'admin0', 'admin1', 'admin2', 'geo1',
                'econ1', 'econ2', 'econ3', 'econ4', 'concept_code',
                'econ_rev', 'econ_rev_sub', 'exclude_from_total', 'approved', 'executed']


# Inference would read entity codes like 110505000 as doubles ("1.10505E8")
CATEGORY_SCHEMA = "econ_rev STRING, econ_rev_sub STRING, exclude_from_total BOOLEAN"
CENTRAL_SCHEMA = (f"year INT, econ1 STRING, econ2 STRING, econ3 STRING, econ4 STRING, "
                  f"approved DOUBLE, executed DOUBLE, {CATEGORY_SCHEMA}")
SUBNATIONAL_SCHEMA = (f"year INT, codigo_fut STRING, entity_name STRING, concept_code STRING, "
                      f"econ1 STRING, econ2 STRING, econ3 STRING, econ4 STRING, "
                      f"approved DOUBLE, executed DOUBLE, {CATEGORY_SCHEMA}")
ROOT_SCHEMA = "year INT, codigo_fut STRING, approved DOUBLE, executed DOUBLE"


def read_csvs(pattern, schema):
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .schema(schema)
      .load(f'{COUNTRY_MICRODATA_DIR}/{pattern}'))

# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name='col_rev_central_bronze')
def col_rev_central_bronze():
    return read_csvs('revenue_central_*.csv', CENTRAL_SCHEMA)


@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name='col_rev_subnat_bronze')
def col_rev_subnat_bronze():
    # "2*" skips the revenue_subnational_root_* check files
    return read_csvs('revenue_subnational_2*.csv', SUBNATIONAL_SCHEMA)


@dlt.table(name='col_rev_subnat_silver')
def col_rev_subnat_silver():
    # Same FUT code → DANE nso_code rule as transform_load_raw_dlt.py
    adm_lookup = spark.table('indicator_intermediate.col_subnational_adm2_adm1_lookup')
    return (dlt.read('col_rev_subnat_bronze')
      .withColumn("nso_code_padded", expr("substring(codigo_fut, length(codigo_fut)-4, 5)").cast('integer'))
      .withColumn(
        "nso_code",
        when(col("nso_code_padded") % 1000 == 0, col("nso_code_padded") / 1000)
        .otherwise(col("nso_code_padded"))
        .cast('integer'))
      .join(adm_lookup, on=["nso_code"], how="left"))

# COMMAND ----------

@dlt.table(name='col_boost_rev_gold')
def col_boost_rev_gold():
    central = (dlt.read('col_rev_central_bronze')
      .withColumn('country_name', lit(COUNTRY))
      .withColumn('admin0', lit('Central'))
      .withColumn('admin1', lit('Central Scope'))
      .withColumn('admin2', lit(None).cast('string'))
      .withColumn('geo1', lit('Central Scope'))
      .withColumn('concept_code', lit(None).cast('string'))
      .select(GOLD_COLUMNS))
    regional = (dlt.read('col_rev_subnat_silver')
      .withColumn('country_name', lit(COUNTRY))
      .withColumn('admin0', lit('Regional'))
      .withColumn('admin1', col('adm1_name'))
      .withColumn('admin2', col('adm2_name'))
      .withColumn('geo1', col('adm1_name'))
      .select(GOLD_COLUMNS))
    return central.unionByName(regional)

# COMMAND ----------

@dlt.expect_or_fail("leaves match root rows", "abs(executed_leaves - executed_root) <= root_tolerance")
@dlt.expect_or_fail("every regional row has a department", "rows_without_admin1 = 0")
@dlt.table(name='col_rev_quality')
def col_rev_quality():
    root = (read_csvs('revenue_subnational_root_*.csv', ROOT_SCHEMA)
      .groupBy('year')
      .agg(sum('executed').alias('executed_root')))
    leaves = (dlt.read('col_rev_subnat_silver')
      .groupBy('year')
      .agg(sum('executed').alias('executed_leaves'),
           sum(when(col('adm1_name').isNull(), 1).otherwise(0)).alias('rows_without_admin1')))
    return (leaves.join(root, on='year', how='full')
      .withColumn('root_tolerance', spark_abs(col('executed_root')) * lit(ROOT_RELATIVE_TOLERANCE)))
