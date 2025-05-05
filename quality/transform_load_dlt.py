# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
CCI_CSV_DIR = f'{WORKSPACE_DIR}/cci_csv'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

# COMMAND ----------

@dlt.table(name=f'quality_cci_bronze')
def quality_cci_bronze():
    ref_csv = f"{CCI_CSV_DIR}/COL/Approved.csv"
    ref_header = spark.read.text(ref_csv).first()[0]
    header_columns = ref_header.split(",")
    schema = StructType([StructField(header_columns[0], StringType())] +
                        [StructField(col, StringType()) for col in header_columns[1:]])
    countries = spark.table('indicator.country').select('country_code', 'country_name')
    
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .schema(schema)
      .load(f'{CCI_CSV_DIR}/*/')
      .withColumn("path_splitted", F.split(F.col("_metadata.file_path"), "/"))
      .withColumn("approved_or_executed", F.regexp_replace(F.element_at(F.col("path_splitted"), -1), "\.csv", ""))
      .withColumn("country_code", F.element_at(F.col("path_splitted"), -2))
      .join(countries, on=["country_code"], how="left")
    )

# COMMAND ----------

@dlt.table(name=f'quality_total_silver')
def quality_total_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category') == 'Spending: Total Expenditures')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f'quality_total_gold')
def quality_total_gold():
    quality_total_silver = dlt.read('quality_total_silver')
    no_budget_and_spending_rows = (
        quality_total_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "year")
        .count()
        .filter(F.col("count") == 2)
    )

    return quality_total_silver.join(no_budget_and_spending_rows, ["country_name", "year"], "leftanti")

# COMMAND ----------

@dlt.table(name=f'quality_functional_silver')
def quality_functional_silver():
    udf_capitalize  = F.udf(lambda x: str(x).capitalize(), StringType())
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code').startswith('EXP_FUNC_'))
        .withColumn('func',
            F.when(
                F.col("category_code") == 'EXP_FUNC_GEN_PUB_SER_EXE' , "General public services"
            ).when(
                F.col("category_code") == 'EXP_FUNC_DEF_EXE' , "Defence"
            ).when(
                F.col("category_code") == 'EXP_FUNC_PUB_ORD_SAF_EXE' , "Public order and safety"
            ).when(
                F.col("category_code") == 'EXP_FUNC_ECO_REL_EXE' , "Economic affairs"
            ).when(
                F.col("category_code") == 'EXP_FUNC_ENV_PRO_EXE' , "Environmental protection"
            ).when(
                F.col("category_code") == 'EXP_FUNC_HOU_EXE' , "Housing and community amenities"
            ).when(
                F.col("category_code") == 'EXP_FUNC_HEA_EXE' , "Health"
            ).when(
                F.col("category_code") == 'EXP_FUNC_REV_CUS_EXC_EXE' , "Recreation, culture and religion"
            ).when(
                F.col("category_code") == 'EXP_FUNC_EDU_EXE' , "Education"
            ).when(
                F.col("category_code") == 'EXP_FUNC_SOC_PRO_EXE' , "Social protection"
            )
        )
        .filter(F.col('func').isNotNull())
        .melt(ids=["country_name", "approved_or_executed", "func"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f'quality_functional_gold')
def quality_functional_gold():
    quality_functional_silver = dlt.read('quality_functional_silver')
    no_budget_and_spending_func_rows = (
        quality_functional_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "func", "year")
        .count()
        .filter(F.col("count") == 2)
    )

    return quality_functional_silver.join(no_budget_and_spending_func_rows, ["country_name", "func", "year"], "leftanti")

# COMMAND ----------

@dlt.table(name=f'quality_functional_sub_silver')
def quality_sub_functional_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())

    return (bronze
        .filter(F.col('category_code').startswith('EXP_FUNC_'))
        .withColumn('func_sub',
            F.when(F.col("category_code") == 'EXP_FUNC_AGR_EXE', "Agriculture")
            .when(F.col("category_code") == 'EXP_FUNC_AIR_TRA_EXE', "Air Transport")
            .when(F.col("category_code") == 'EXP_FUNC_ENE_EXE', "Energy")
            .when(F.col("category_code") == 'EXP_FUNC_JUD_EXE', "Judiciary")
            .when(F.col("category_code") == 'EXP_FUNC_PRI_EDU_EXE', "Primary Education")
            .when(F.col("category_code") == 'EXP_FUNC_PRI_HEA_EXE', "Primary and Secondary Health")
            .when(F.col("category_code") == 'EXP_FUNC_PUB_SAF_EXE', "Public Safety")
            .when(F.col("category_code") == 'EXP_FUNC_ROA_EXE', "Roads")
            .when(F.col("category_code") == 'EXP_FUNC_RAI_EXE', "Railroads")
            .when(F.col("category_code") == 'EXP_FUNC_TEL_EXE', "Telecom")
            .when(F.col("category_code") == 'EXP_FUNC_SEC_EDU_EXE', "Secondary Education")
            .when(F.col("category_code") == 'EXP_FUNC_TER_EDU_EXE', "Tertiary Education")
            .when(F.col("category_code") == 'EXP_FUNC_TER_HEA_EXE', "Tertiary and Quaternary Health")
            .when(F.col("category_code") == 'EXP_FUNC_TRA_EXE', "Transport")
            .when(F.col("category_code") == 'EXP_FUNC_WAT_TRA_EXE', "Water Transport")
            .when(F.col("category_code") == 'EXP_FUNC_WAT_SAN_EXE', "Water and Sanitation")
        )
        .filter(F.col('func_sub').isNotNull())
        .melt(ids=["country_name", "approved_or_executed", "func_sub"], 
              values=year_cols, 
              variableColumnName="year", 
              valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )


# COMMAND ----------

@dlt.table(name=f'quality_functional_sub_gold')
def quality_functional_sub_gold():
    quality_econ_silver = dlt.read('quality_functional_sub_silver')
    no_budget_and_spending_econ_rows = (
        quality_econ_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "func_sub", "year")
        .count()
        .filter(F.col("count") == 2)
    )
    return quality_econ_silver.join(no_budget_and_spending_econ_rows, ["country_name", "func_sub",  "year"], "leftanti") 

# COMMAND ----------

@dlt.table(name=f'quality_economic_silver')
def quality_economic_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code').startswith('EXP_ECON_'))
        .withColumn('econ',
            F.when(
                F.col("category_code") == 'EXP_ECON_WAG_BIL_EXE' , "Wage bill"
            ).when(
                F.col("category_code") == 'EXP_ECON_CAP_EXP_EXE' , "Capital expenditures"
            ).when(
                F.col("category_code") == 'EXP_ECON_USE_GOO_SER_EXE' , "Goods and services"
            ).when(
                F.col("category_code") == 'EXP_ECON_SUB_EXE' , "Subsidies"
            ).when(
                F.col("category_code") == 'EXP_ECON_SOC_BEN_EXE' , "Social benefits"
            ).when(
                F.col("category_code") == 'EXP_ECON_OTH_GRA_EXE' , "Other grants and transfers"
            ).when(
                F.col("category_code") == 'EXP_ECON_OTH_EXP_EXE' , "Other expenses"
            ).when(
                F.col("category_code") == 'EXP_ECON_INT_DEB_EXE' , "Interest on debt"
            )
        )
        .filter(F.col('econ').isNotNull())
        .melt(ids=["country_name", "approved_or_executed", "econ"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f'quality_economic_gold')
def quality_economic_gold():
    quality_econ_silver = dlt.read('quality_economic_silver')
    no_budget_and_spending_econ_rows = (
        quality_econ_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "econ", "year")
        .count()
        .filter(F.col("count") == 2)
    )
    return quality_econ_silver.join(no_budget_and_spending_econ_rows, ["country_name", "econ",  "year"], "leftanti")


# COMMAND ----------

@dlt.table(name=f'quality_economic_sub_silver')
def quality_economic_sub_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())

    return (bronze
        .filter(F.col('category_code').startswith('EXP_ECON_'))
        .withColumn('econ_sub',
            F.when(F.col("category_code") == 'EXP_ECON_ALL_EXE', "Allowances")
            .when(F.col("category_code") == 'EXP_ECON_GOO_SER_BAS_SER_EXE', "Basic Services")
            .when(F.col("category_code") == 'EXP_ECON_BAS_WAG_EXE', "Basic Wages")
            .when(F.col("category_code") == 'EXP_ECON_CAP_EXP_FOR_EXE', "Capital Expenditure (foreign spending)")
            .when(F.col("category_code") == 'EXP_ECON_CAP_MAI_EXE', "Capital Maintenance")
            .when(F.col("category_code") == 'EXP_ECON_GOO_SER_EMP_CON_EXE', "Employment Contracts")
            .when(F.col("category_code") == 'EXP_ECON_OTH_SOC_BEN_EXE', "Other Social Benefits")
            .when(F.col("category_code") == 'EXP_ECON_SOC_BEN_PEN_EXE', "Pensions")
            .when(F.col("category_code") == 'EXP_ECON_REC_MAI_EXE', "Recurrent Maintenance")
            .when(F.col("category_code") == 'EXP_ECON_SOC_ASS_EXE', "Social Assistance")
            .when(F.col("category_code") == 'EXP_ECON_PEN_CON_EXE', "Social Benefits (pension contributions)")
            .when(F.col("category_code") == 'EXP_ECON_SUB_PRO_EXE', "Subsidies to Production")
        )
        .filter(F.col('econ_sub').isNotNull())
        .melt(ids=["country_name", "approved_or_executed", "econ_sub"], 
              values=year_cols, 
              variableColumnName="year", 
              valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f'quality_economic_sub_gold')
def quality_economic_sub_gold():
    quality_econ_silver = dlt.read('quality_economic_sub_silver')
    no_budget_and_spending_econ_rows = (
        quality_econ_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "econ_sub", "year")
        .count()
        .filter(F.col("count") == 2)
    )
    return quality_econ_silver.join(no_budget_and_spending_econ_rows, ["country_name", "econ_sub",  "year"], "leftanti") 

# COMMAND ----------

# Exploratory for Manuel. Remove if they are not going to use
@dlt.table(name=f'quality_judiciary_silver')
def quality_judiciary_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category') == 'Spending in judiciary')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f"quality_judiciary_gold")
def quality_judiciary_gold():
    quality_judiciary_silver = dlt.read('quality_judiciary_silver')
    no_budget_and_spending_rows = (
        quality_judiciary_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "year")
        .count()
        .filter(F.col("count") == 2)
    )
    return quality_judiciary_silver.join(
        no_budget_and_spending_rows, ["country_name", "year"], "leftanti"
    )

# COMMAND ----------


@dlt.table(name=f'quality_total_subnat_silver')
def quality_total_subnat_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code') == 'EXP_ECON_SBN_TOT_SPE_EXE')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f"quality_total_subnat_gold")
def quality_total_subnat_gold():
    quality_total_subnat_silver = dlt.read("quality_total_subnat_silver")
    no_budget_and_spending_rows = (
        quality_total_subnat_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "year")
        .count()
        .filter(F.col("count") == 2)
    )
    return quality_total_subnat_silver.join(
        no_budget_and_spending_rows, ["country_name", "year"], "leftanti"
    )

# COMMAND ----------

@dlt.table(name=f'quality_total_foreign_silver')
def quality_total_foreign_silver():
    bronze = dlt.read('quality_cci_bronze')
    year_cols = list(col_name for col_name in bronze.columns if col_name.isnumeric())
    return (bronze
        .filter(F.col('category_code') == 'EXP_ECON_TOT_EXP_FOR_EXE')
        .melt(ids=["country_name", "approved_or_executed"], 
            values=year_cols, 
            variableColumnName="year", 
            valueColumnName="amount"
        )
        .filter(F.col('amount').isNotNull())
    )

# COMMAND ----------

@dlt.table(name=f"quality_total_foreign_gold")
def quality_total_foreign_gold():
    quality_total_foreign_silver = dlt.read("quality_total_foreign_silver")
    no_budget_and_spending_rows = (
        quality_total_foreign_silver.filter(F.col("amount") == 0)
        .groupBy("country_name", "year")
        .count()
        .filter(F.col("count") == 2)
    )
    return quality_total_foreign_silver.join(
        no_budget_and_spending_rows, ["country_name", "year"], "leftanti"
    )
