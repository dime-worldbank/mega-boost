# Databricks notebook source
import dlt
import json
from pyspark.sql.functions import col, when, lit, substring, udf
from pyspark.sql.types import StringType
from glob import glob


# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Albania'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'
RAW_COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'
RAW_INPUT_DIR = f"{TOP_DIR}/Documents/input/Data from authorities/"

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

with open(f"{RAW_INPUT_DIR}/{COUNTRY}/labels_en_v01_overall.json", 'r') as json_file:
    labels = json.load(json_file)

def replacement_udf(column_name):
    def replace_value(value):
        if value is None:
            return value
        value_str = str(value).split('.')[0]
        return labels.get(column_name, {}).get(value_str, value_str)
    return udf(replace_value, StringType())


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'alb_2022_and_before_boost_rev_bronze')
def boost_rev_bronze():
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Data_Revenues.csv')
            .filter(col("year") < 2023)
    )


@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'alb_2023_onward_boost_rev_bronze')
def boost_2023_onward_rev_bronze():
    file_paths = [x for x in glob(f"{RAW_COUNTRY_MICRODATA_DIR}/*.csv") if 'rev' in x]
    bronze_df = None
    for f in file_paths:
        df = (spark.read
              .format("csv")
              .options(**CSV_READ_OPTIONS)
              .option("inferSchema", "true")
              .option("header", "true")
              .load(f))
        if bronze_df is None:
            bronze_df = df
        else:
            bronze_df = bronze_df.unionByName(df, allowMissingColumns=True)
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df


@dlt.table(name=f'alb_2023_onward_boost_rev_silver')
def boost_silver():
    silver_df  = (dlt.read(f'alb_2023_onward_boost_rev_bronze')
                .withColumn("econ1", substring("econ5", 1, 1))
                .withColumn("econ2", substring("econ5", 1, 2))
                .withColumn("econ3", substring("econ5", 1, 3))
                .withColumn("econ4", substring("econ5", 1, 4))
                .withColumn('admin1', substring("admin4", 1, 1))
                .filter(~((col("econ2") == "16") |
                            (col("econ2") == "17") |
                            (col("econ3") == "480") |
                            (col("econ2") == "73") |
                            (col("econ2") == "11")))
                .withColumn("transfer", 
                    when((col("econ5") == "7200105") & (col("admin2") == "001"), 1) # Central government institutions (GE=001)
                    .when(col("econ5") == "7201101", 1) # Transfers to SNG entities
                    .when(col("econ5").isin("7200102", "7200101"), 1) # Region Council -> Municipalities, vs. and between Reg.Counc. & Region Council ->Subordinated Institution of Region Council
                    .when(col("econ5").isin("7200106", "7200107"), 1) # Municipalities, Communes, Regional Counc. -> Central Institutions
                    .when(col("econ4").isin("7202", "7203"), 1) # Central Government-> Social&Health Insurance
                    .when(col("econ3").isin("434", "437"), 1) # Transfers between the Social Security Funds
                    .otherwise(0))
                .filter(
                    ~(
                        ((col("econ2") == 75) | ((col("econ2") == 72) & (col("transfer") == 0)))
                        & ~( ((col("admin2") == 3) | (col("admin2") == 4)) & (col("admin3") == 0) )
                    )
                ).filter(col("econ1").isNotNull())
    )

    for column_name, mapping in labels.items():
        if column_name in silver_df.columns:
            silver_df = silver_df.withColumn(column_name, replacement_udf(column_name)(col(column_name)))

    silver_df = (silver_df.filter(col('transfer')=='Excluding transfers')
                .withColumn('admin0',
                    when(col('admin1')=='Central', 'Central')
                    .when(col('admin1')=='Local', 'Regional'))
                 .withColumn('econ_rev_sub',
                    when(col('econ3').startswith('700') & (~col('econ4').startswith('7000')), 'Corporate Income Tax')
                    .when(col('econ4').startswith('7000'), 'Personal Income Tax')
                    .when(col('econ4').startswith('7020'), 'Property Taxes (Immovable)')
                    .when(col('econ4').startswith('7021'), 'Property Taxes (Transactions)')
                    .when(substring(col('econ5'), 1, 7).isin(['7040100', '7049100']), 'Customs (Imports)')
                    .when(substring(col('econ5'), 1, 7).isin(['7041100']), 'Customs (Exports)')
                    .when(substring(col('econ5'), 1, 7).isin(['7031400', '7031500']), 'Excises (Fuels)')
                    .when(substring(col('econ5'), 1, 7).isin(['7031100']), 'Excises (Tobacco)')
                    .when(substring(col('econ5'), 1, 7).isin(['7031200', '7031300']), 'Excises (Alcohol)'))
                 .withColumn('econ_rev',
                    when(col('econ3').startswith('700'), 'Income Tax')
                    .when(col('econ4').startswith('7030'), 'VAT')
                    .when(col('econ4').startswith('7031'), 'Excises')
                    .when(col('econ3').startswith('704'), 'Customs')
                    .when(substring(col('econ5'), 1, 7).isin(['7033012', '7109300']), 'Royalties')
                    .when(substring(col('econ4'), 1, 4).isin(['7110', '7111', '7112', '7115']), 'Permits and Fees')
                    .when(col('econ3').startswith('435'), 'Social Contributions')
                    .when(col('econ3').startswith('702'), 'Property Taxes')
                    .when(col('admin1')=='Local', 'Subnational Own Revenues')))
    return silver_df


@dlt.table(name=f'alb_2022_and_before_boost_rev_silver')
def boost_rev_silver():
    return (dlt.read(f'alb_2022_and_before_boost_rev_bronze')
                .withColumn('admin0',
                    when(col('admin1')=='Central', 'Central')
                    .when(col('admin1')=='Local', 'Regional'))
                .filter(col('transfer')=='Excluding transfers')      
                .withColumn('econ_rev_sub',
                    when(col('econ3').startswith('700') & (~col('econ4').startswith('7000')), 'Corporate Income Tax')
                    .when(col('econ4').startswith('7000'), 'Personal Income Tax')
                    .when(col('econ4').startswith('7020'), 'Property Taxes (Immovable)')
                    .when(col('econ4').startswith('7021'), 'Property Taxes (Transactions)')
                    .when(substring(col('econ5'), 1, 7).isin(['7040100', '7049100']), 'Customs (Imports)')
                    .when(substring(col('econ5'), 1, 7).isin(['7041100']), 'Customs (Exports)')
                    .when(substring(col('econ5'), 1, 7).isin(['7031400', '7031500']), 'Excises (Fuels)')
                    .when(substring(col('econ5'), 1, 7).isin(['7031100']), 'Excises (Tobacco)')
                    .when(substring(col('econ5'), 1, 7).isin(['7031200', '7031300']), 'Excises (Alcohol)'))
                .withColumn('econ_rev',
                    when(col('econ3').startswith('700'), 'Income Tax')
                    .when(col('econ4').startswith('7030'), 'VAT')
                    .when(col('econ4').startswith('7031'), 'Excises')
                    .when(col('econ3').startswith('704'), 'Customs')
                    .when(substring(col('econ5'), 1, 7).isin(['7033012', '7109300']), 'Royalties')
                    .when(substring(col('econ4'), 1, 4).isin(['7110', '7111', '7112', '7115']), 'Permits and Fees')
                    .when(col('econ3').startswith('435'), 'Social Contributions')
                    .when(col('econ3').startswith('702'), 'Property Taxes')
                    .when(col('admin1')=='Local', 'Subnational Own Revenues'))
            )


@dlt.table(name=f'alb_2022_and_before_boost_rev_gold')
def alb_2022_and_before_boost_rev_gold():
    return (dlt.read(f'alb_2022_and_before_boost_rev_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'admin0',
                'admin1',
                'admin2',
                'admin3',
                'admin4',
                'econ1',
                'econ2',
                'econ3',
                'econ4',
                'econ5',
                'econ_rev_sub',
                'econ_rev',
                'executed'))


@dlt.table(name=f'alb_2023_onward_boost_rev_gold')
def alb_2023_onward_boost_rev_gold():
    return (dlt.read(f'alb_2023_onward_boost_rev_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'admin0',
                'admin1',
                'admin2',
                'admin3',
                'admin4',
                'econ1',
                'econ2',
                'econ3',
                'econ4',
                'econ5',
                'econ_rev_sub',
                'econ_rev',
                'executed'))


@dlt.table(name="alb_boost_rev_gold")
def alb_boost_rev_gold():
    df_before_2023 = dlt.read("alb_2022_and_before_boost_rev_gold")
    df_from_2023 = dlt.read("alb_2023_onward_boost_rev_gold")
    return df_before_2023.unionByName(df_from_2023)         
