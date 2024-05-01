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

def contains_any(column, words_to_check):
    filter_condition = None
    for word in words_to_check:
        if filter_condition is None:
            filter_condition = lower(column).contains(word)
        else:
            filter_condition = filter_condition | lower(column).contains(word)
    return filter_condition

@dlt.table(name=f'ken_boost_silver')
def boost_silver():
    culture_keywords = ["sports", "culture", "heritage", "library", "arts"]
    housing_keywords_01 = [
        "housing development and human settlement",
    ]
    housing_keywords_10 = [
        "integrated regional development",
        "water policy and management",
        "water supply services",
        "water resources management",
    ]
    return (dlt.read(f'ken_boost_bronze')
        .filter(~col('Class').isin('2 Revenue', '4 Funds & Deposits (BTL)'))
        .filter(~col('Chapter_econ2').isin(
            '52 Settlement Of Financial Liabilities',
            '55 Settlement Of Financial Liabilities',
        ))
        .withColumn('adm1_name', 
            when(lower(col("Counties_Geo2")).like("%county%"),
                 initcap(trim(regexp_replace(regexp_replace(col("Counties_Geo2"), "\d+", ""), "County", ""))))
            .when(lower(col("Counties_Geo2")).like("%nation%"), 'Central Scope')
        ).withColumn('adm1_name',
            when(col("adm1_name") == 'Muranga', "Murang’A")
            .when(col("adm1_name") == "Transnzoia",  'Trans Nzoia')
            .otherwise(col("adm1_name"))
        ).withColumn( 'admin0',
            when(col('Vote_Groups_adm1') == '3 Counties', 'Regional')
            .otherwise('Central')
        ).withColumn('admin1',
            when(col('admin0')=='Central', 'Central')
            .otherwise(trim(regexp_replace(col("National_Government_Votes_&_Counties_adm2"), '^[0-9\\s]*', '')))
        ).withColumn('admin2',
            trim(regexp_replace(col("National_Government_Votes_&_Counties_adm2"), '^[0-9\\s]*', ''))
        ).withColumn('geo1', 
            when(col('admin0') == 'Regional', col('admin1')) # if spent by Regional government then it is assumed to be spent in a region
            .when((col('admin0') == 'Central') & lower(col("Counties_Geo2")).like("%county%"), 
                initcap(trim(regexp_replace(col("Counties_Geo2"), "\d+", "")))) # central spending tagged with a region
            .otherwise('Central Scope') # central spending not linked to a region
        ).withColumn('year', concat(lit('20'), substring(col('Year'), -2, 2)).cast('int')
        ).withColumn('is_foreign', (~col('Class').startswith('2')) & (~col('SOF2').startswith('00'))
        ).withColumn('func_sub',
            when(
                (col('Sector_prog1').startswith('06') & 
                 (col('National_Government_Votes_&_Counties_adm2').startswith('102') |
                  col('National_Government_Votes_&_Counties_adm2').startswith('210') |
                  col('National_Government_Votes_&_Counties_adm2').startswith('215'))),
                "public safety")
            .when(
                col('Sector_prog1').startswith('06'), 
                "judiciary") # important for this to be after public safety
            .when(
                col('Programme_pro2').startswith('0401'), 'primary and secondary health')
            .when(
                col('Programme_pro2').startswith('0402'), 'tertiary and quaternary health')
            .when(
                (col('Sector_prog1').startswith('05') & (
                    col('Programme_pro2').startswith('0501 Primary') |
                    col('Programme_pro2').startswith('0502 Basic') |
                    col('Sub-programme_prog3').startswith('050901'))
                ), 'primary education')
            .when(
                (col('Sector_prog1').startswith('05') & (
                    col('Programme_pro2').startswith('0502 secondary') |
                    col('Sub-programme_prog3').startswith('050902'))
                ), 'secondary education')
            .when(
                (col('Sector_prog1').startswith('05') & (
                    col('Programme_pro2').startswith('0504') |
                    col('Programme_pro2').startswith('0505') |
                    col('Sub-programme_prog3').startswith('05093'))
                ), 'tertiary education'
            )
        ).withColumn('func', 
            when(
                (col('Sector_prog1').startswith('09') &
                 contains_any(col('Programme_pro2'), culture_keywords)),
                'Recreation, culture and religion')
            .when(
                (col('Item_econ4').startswith('27101') | 
                 col('Sector_prog1').startswith('09')),
                'Social protection') # This needs to be after 'Recreation, culture and religion' as Sector_prog1 09 includes both
                # This also needs to be before almost everything else that rely on Sector_prog1 because it uses econ4 which can be cross sector
            .when(
                col('Sector_prog1').startswith('07'),
                'General public services')
            .when(
                col('Sector_prog1').startswith('08'),
                'Defense') # Note: Defence has no allocated amount in the executed sheet
            .when(
                col("func_sub").isin("judiciary", "public safety"),
                "Public order and safety")
            .when(
                col('Programme_pro2').isin([
                    '1002 Environment Management and Protection',
                    '1007 Environment Management and Protection'
                ]),
                'Environmental protection')
            .when(
                (((col('Sector_prog1').startswith('01') &
                 contains_any(col('Programme_pro2'), housing_keywords_01))) |
                 (col('Sector_prog1').startswith('10') &
                 contains_any(col('Programme_pro2'), housing_keywords_10))),
                "Housing and community amenities")
            .when(
                col('Sector_prog1').startswith('04'),
                'Health')
            .when(
                col('Sector_prog1').startswith('05'),
                'Education') # 2015 doesn't match
            .when(
                (col('Sector_prog1').startswith('01') |
                col('Sector_prog1').startswith('02') | 
                col('Sector_prog1').startswith('03') | 
                col('Sector_prog1').startswith('10')),
                'Economic affairs') # This needs to be after Environment and housing to catch the rest of Sector_prog1 startin with 10 
            # Sector_prog1 = 00 Default - Non Programmatic are not tagged
        ).withColumn('econ_sub',
            # basic wages computed after computing econ categories
            # social benefits (pension contributions)
            when((col('Item_econ4').startswith('21201')), 'social benefits (pension contributions)')
            # employment contracts
            .when((col('Sub-Item_econ5').startswith('2211310')| col('Sub-Item_econ5').startswith('2110201')), 'employment contracts')
            # allowances
            .when(((col('Class').startswith('0')) & (col('Item_econ4').startswith('21103'))), 'allowances')
            # foreign funded cap ex
            .when((col('Chapter_econ2').startswith('31') & (~col('SOF2').startswith('00'))), 'capital expenditure (foreign spending)')
            # basic services            
            .when(
                ((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('22')) & (
                    (col('Sub-Item_econ5').startswith('2211201')) | 
                    (col('Sub-Item_econ5').startswith('2210201')) |
                    (col('Sub-Item_econ5').startswith('2210102')) |
                    (col('Sub-Item_econ5').startswith('2210101')) |
                    (col('Sub-Item_econ5').startswith('2211015'))
                )), 'basic services')
            # recurrent maintenance
            .when(col('Item_econ4').startswith('22202'), 'recurrent maintenance')
            # subsidies to production
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('25'))), 'subsidies to production')
            #pensions
            .when(((~col('Class').startswith('2')) & (col('Item_econ4').startswith('27101'))), 'pensions')
            # social assistance
            .when(
                ((~col('Class').startswith('2')) & (~col('Class').startswith('4')) & (~col('Item_econ4').startswith('27101')) & (col('Sector_prog1').startswith('09'))), 'social assistance')
        ).withColumn('econ',
            # wage bill
            when((
                    ((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('21'))) | 
                    ((col('Item_econ4').startswith('22103') | col('Item_econ4').startswith('22104')  | col('Item_econ4').startswith('22107')) & (col('Class').startswith('0'))) |
                    ((col('Item_econ4').startswith('22103') | col('Item_econ4').startswith('22104')  | col('Item_econ4').startswith('22107')) & (col('Class').startswith('1')))                   
                ), 'Wage bill')
            # cap ex
            .when(col('Chapter_econ2').startswith('31'), 'Capital expenditure')
            # Goods and services
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('22'))), 'Goods and services')
            # subsidies           
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('25'))), 'Subsidies')
            # social benefits
            .when(col("econ_sub").isin("social assistance", "pensions"), 'Social benefits')
            # other grants and transfers category not available
            # other expenses
            .otherwise('Other expenses')
        ).withColumn('econ_sub', # defined as the difference of wage bill and allowances
            when(((col('econ') == 'Wage bill') & (~col('econ_sub').isin( 'allowances', 'social benefits (pension contributions)'))), 'basic wages')
            .when((col('econ') == 'Wage bill') & (col('econ_sub').isNull()), 'basic wages')
            .otherwise(col('econ_sub'))
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
                'admin0',
                'admin1',
                'admin2',
                'geo1',
                'is_foreign',
                'func',
                'func_sub',
                'econ',
                'econ_sub'
                )
    )
