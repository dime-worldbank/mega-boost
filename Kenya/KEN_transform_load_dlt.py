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
        .filter(~col('Class').isin('2 Revenue', '4 Funds & Deposits (BTL)'))
        .withColumn('adm1_name', 
            when(lower(col("Counties_Geo2")).like("%county%"),
                 initcap(trim(regexp_replace(regexp_replace(col("Counties_Geo2"), "\d+", ""), "County", "")))
            ).when(lower(col("Counties_Geo2")).like("%nation%"), 'Central Scope')
        ).withColumn('adm1_name',
            when(col("adm1_name") == 'Muranga', "Murang’A")
            .when(col("adm1_name") == "Transnzoia",  'Trans Nzoia')
            .otherwise(col("adm1_name"))
        ).withColumn('year', concat(lit('20'), substring(col('Year'), -2, 2)).cast('int')
        ).withColumn('func_sub',
            when(
                (col('Sector_prog1').startswith('06') & 
                 (col('National_Government_Votes_&_Counties_adm2').startswith('102') |
                  col('National_Government_Votes_&_Counties_adm2').startswith('210') |
                  col('National_Government_Votes_&_Counties_adm2').startswith('215'))),
                "public safety"
            ).when(
                col('Sector_prog1').startswith('06'), 
                "judiciary" # important for this to be after public safety
            )
        ).withColumn('func', 
            when(
                (col('Programme_pro2').startswith('0901 Sports') | 
                col('Programme_pro2').startswith('0902 Culture') | 
                col('Programme_pro2').startswith('0903 The Arts') | 
                col('Programme_pro2').startswith('0904 Library Services') | 
                col('Programme_pro2').startswith('0905 National Heritage and Culture')),
                'Recreation, culture and religion'
            ).when(
                (col('Item_econ4').startswith('27101') | 
                 (col('Sector_prog1').startswith('09') & ~col('Programme_pro2').endswith('Manpower Development, Employment and Productivity Management'))),
                'Social protection' # This needs to be after 'Recreation, culture and religion' as Sector_prog1 09 includes both
                # This also needs to be before almost everything else that rely on Sector_prog1 because it uses econ4 which can be cross sector
            ).when(
                col('Sector_prog1').startswith('07'),
                'General public services'
            ).when(
                col('Sector_prog1').startswith('08'),
                'Defense' # Note: Defence has no allocated amount in the executed sheet
            ).when(
                col("func_sub").isin("judiciary", "public safety"),
                "Public order and safety"
            ).when(
                col('Programme_pro2').isin([
                    '1002 Environment Management and Protection',
                    '1007 Environment Management and Protection'
                ]),
                'Environmental protection'
            ).when(
                (col('Programme_pro2').endswith('Housing Development and Human Settlement') | 
                col('Programme_pro2').endswith('Integrated Regional Development') | 
                (col('Sector_prog1').startswith('10') & lower(col('Programme_pro2')).like('%water%'))),
                "Housing and community amenities"
            ).when(
                col('Sector_prog1').startswith('04'),
                'Health'
            ).when(
                col('Sector_prog1').startswith('05'),
                'Education' # 2015 doesn't match
            ).when(
                (col('Sector_prog1').startswith('01') |
                col('Sector_prog1').startswith('02') | 
                col('Sector_prog1').startswith('03') | 
                col('Sector_prog1').startswith('10') |
                col('Programme_pro2').endswith('Manpower Development, Employment and Productivity Management')),
                'Economic affairs' # This needs to be after Environment and housing to catch the rest of Sector_prog1 startin with 10
            ) 
            # Sector_prog1 = 00 Default - Non Programmatic are not tagged
        )
    )
    
@dlt.table(name=f'ken_boost_gold')
def boost_gold():
    return (dlt.read(f'ken_boost_silver')
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
