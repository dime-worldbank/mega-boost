# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, concat
from pyspark.sql.types import StringType, DoubleType


# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Kenya'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'ken_boost_bronze')
def boost_bronze():
    # Load the data from CSV
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(COUNTRY_MICRODATA_DIR))
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name=f'ken_boost_silver')
def boost_silver():
    return (dlt.read(f'ken_boost_bronze')
        .withColumn('adm1_name', 
                    when(col("Counties_Geo2").isNotNull(),
                         when(lower(col("Counties_Geo2")).like("%county%"),
                              initcap(trim(regexp_replace(regexp_replace(col("Counties_Geo2"), "\d+", ""), "County", "")))
                         )
                         .when(lower(col("Counties_Geo2")).like("%nation%"), 'Central Scope')
                         .otherwise('Other') # When 0
                    ).otherwise('Other') # When Null
        ).withColumn('adm1_name',
                     when(col("adm1_name") == 'Muranga', "Murang’A")
                    .when(col("adm1_name") == "Transnzoia",  'Trans Nzoia')
                    .otherwise(col("adm1_name"))
        ).withColumn('year', concat(lit('20'), substring(col('Year'), -2, 2)).cast('int')
        ).withColumn('func', 
    # Social protection has double counting of certain rows (it is sum of two quantities that have overlapping rows)
    when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         (col('Class').substr(1, 1) != '4') & 
         col('Item_econ4').startswith('27101')),
        'Social protection'
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         (col('Class').substr(1, 1) != '4') & 
         col('Sector_prog1').startswith('09')),
        'Social protection'
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         col('Sector_prog1').startswith('05')),
        'Education' # 2014/15 doesn't match
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         col('Sector_prog1').startswith('04')),
        'Health'
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         col('Sector_prog1').startswith('06') & 
         ~(col('National_Government_Votes_&_Counties_adm2').startswith('102') | col('National_Government_Votes_&_Counties_adm2').startswith('210') | col('National_Government_Votes_&_Counties_adm2').startswith('215'))),
        'Public order and safety'
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').startswith('0')) & 
         col('Sector_prog1').startswith('06') & 
         (col('National_Government_Votes_&_Counties_adm2').startswith('102') | col('National_Government_Votes_&_Counties_adm2').startswith('210') | col('National_Government_Votes_&_Counties_adm2').startswith('215'))),
        'Public order and safety'
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').startswith('1')) & 
         col('Sector_prog1').startswith('06') & 
         (col('National_Government_Votes_&_Counties_adm2').startswith('102') | col('National_Government_Votes_&_Counties_adm2').startswith('210') | col('National_Government_Votes_&_Counties_adm2').startswith('215'))),
        'Public order and safety' # 2022 doesn't match
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         (col('Class').substr(1, 1) != '4') &  
         col('Sector_prog1').startswith('07')),
        'General public services' # matches only for 2021 and 2022
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         (col('Class').substr(1, 1) != '4') &
         (col('Sector_prog1').startswith('01') | col('Sector_prog1').startswith('02') | col('Sector_prog1').startswith('03') | col('Sector_prog1').startswith('10'))),
        'Economic affairs'
    ).when(
        ((col('Class').isNotNull()) & 
         (col('Class').substr(1, 1) != '2') & 
         (col('Class').substr(1, 1) != '4') &
         (col('Programme_pro2').startswith('0907'))),
        'Economic affairs'
    ).otherwise(lit("Other"))
    )
    )
# Note: Defence has no allocated amount in the executed sheet
# Environment has overlapping categories with pervious COFOG codes
# Housing: The codes seem to change across years and are overlapping with other COFOG codes
# Recreation and culture seems to have overlap with social protection
    
@dlt.table(name=f'ken_boost_gold')
def boost_gold():
    return (dlt.read(f'ken_boost_silver')
        .filter(~col('Class').isin('2 Revenue', '4 Funds & Deposits (BTL)'))
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'adm1_name',
                'year',
                col('Initial_Budget_Printed_Estimate').alias('approved').cast(DoubleType()),
                col('Final_Budget_Approved_Estimate').alias('revised'),
                col('`Final_Expenditure_Total_Payment_Comm.`').alias('executed'),
                'func'
                )
    )
