# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, concat
from pyspark.sql.types import StringType, DoubleType
from glob import glob

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
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

def clean_col_names(df):
    for old_col_name in df.columns:
        new_col_name = old_col_name.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "")
        if new_col_name != old_col_name:
            df = df.withColumnRenamed(old_col_name, new_col_name)
    return df


@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'ken_boost_bronze')
def boost_bronze():
    # We read the files individually and explicitly union them, rather than reading the entire directory at once,
    # to avoid schema mismatches across files that could lead to data corruption.
    file_paths = glob(f"{COUNTRY_MICRODATA_DIR}/*.csv")
    bronze_df = spark.read.format("csv").options(**CSV_READ_OPTIONS).option("inferSchema", "true").load(file_paths[0])
    bronze_df = clean_col_names(bronze_df)
    for f in file_paths[1:]:
        # Load the data from CSV
        df = (spark.read
                    .format("csv")
                    .options(**CSV_READ_OPTIONS)
                    .option("inferSchema", "true")
                    .load(f))
        df = clean_col_names(df)
        bronze_df = bronze_df.unionByName(df, allowMissingColumns=True)
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
    culture_keywords = [" sports", " culture", " heritage", " library", " arts"]
    housing_keywords_01 = [
        "housing development and human settlement",
    ]
    housing_keywords_10 = [
        "integrated regional development",
        "water policy and management",
        "water supply",
        "water resources management",
        "water and sanitation"
    ]
    return (dlt.read(f'ken_boost_bronze')
        .filter(~col('Class').isin('2 Revenue', '4 Funds & Deposits (BTL)'))
        .filter(~col('Chapter_econ2').isin(
            '52 Settlement Of Financial Liabilities',
            '55 Settlement Of Financial Liabilities',
        ))
        .withColumn('admin2',
            regexp_replace(col("National_Government_Votes_&_Counties_adm2"), '^[0-9\\s]*', '')
        ).withColumn('admin2',
            trim(regexp_replace(col("admin2"), 'County', ''))
        ).withColumn('admin2',
            when(col("admin2") == 'Muranga', "Murang’A")
            .otherwise(col("admin2"))
        ).withColumn('county_geo', 
            when(
                lower(col("Counties_Geo2")).like("%county%"),
                initcap(trim(regexp_replace(regexp_replace(col("Counties_Geo2"), "\d+", ""), "County", "")))
            ).otherwise('Central')
        ).withColumn('county_geo',
            when(col("county_geo") == 'Muranga', "Murang’A")
            .when(col("county_geo") == "Transnzoia",  'Trans Nzoia')
            .otherwise(col("county_geo"))
        ).withColumn('admin0',
            when(col('Vote_Groups_adm1') == '3 Counties', 'Regional')
            .otherwise('Central')
        ).withColumn('admin1',
            when(col('admin0')=='Central', 'Central Scope')
            .otherwise(col('admin2'))
        ).withColumn('geo1', 
            # if spent by Regional government then it is assumed to be spent in a region
            when(col('admin0') == 'Regional', col('admin1'))
            .when( # central spending tagged with a region
                (col('admin0') == 'Central') & (col("county_geo") != "Central"), 
                col('county_geo')
            ).otherwise('Central Scope') # central spending not linked to a region
        ).withColumn('year', concat(lit('20'), substring(col('Year'), -2, 2)).cast('int')
        ).withColumn('is_foreign', ((~col('SOF2').startswith('00') | col('SOF2').isNull()))
        ).withColumn('func_sub',
            when(
                (col('Sector_prog1').startswith('06') & 
                 (col('National_Government_Votes_&_Counties_adm2').startswith('102') |
                  col('National_Government_Votes_&_Counties_adm2').startswith('210') |
                  col('National_Government_Votes_&_Counties_adm2').startswith('215'))),
                "Public Safety")
            .when(
                col('Sector_prog1').startswith('06'), 
                "Judiciary") # important for this to be after Public Safety
            .when(
                col('Programme_pro2').startswith('0401'), 'Primary and Secondary Health')
            .when(
                col('Programme_pro2').startswith('0402'), 'Tertiary and Quaternary Health')
            .when(
                (col('Sector_prog1').startswith('05') & (
                    col('Programme_pro2').startswith('0501 Primary') |
                    col('Programme_pro2').startswith('0502 Basic') |
                    col('Sub-programme_prog3').startswith('050901'))
                ), 'Primary Education')
            .when(
                (col('Sector_prog1').startswith('05') & (
                    col('Programme_pro2').startswith('0502 secondary') |
                    col('Sub-programme_prog3').startswith('050902'))
                ), 'Secondary Education')
            .when(
                (col('Sector_prog1').startswith('05') & (
                    col('Programme_pro2').startswith('0504') |
                    col('Programme_pro2').startswith('0505') |
                    col('Sub-programme_prog3').startswith('05093'))
                ), 'Tertiary Education'
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
                col('Sector_prog1').startswith('08'),
                'Defence') # Note: Defence has no allocated amount in the executed sheet
            .when(
                col("func_sub").isin("Judiciary", "Public Safety"),
                "Public order and safety")
            .when(
                ~col('environment').isNull(),
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
            .otherwise('General public services')
            # Sector_prog1 = 00 Default - Non Programmatic are not tagged
        ).withColumn('econ_sub',
            # Social Assistance
            when(
                ((~col('Class').startswith('2')) & (~col('Class').startswith('4')) & (~col('Item_econ4').startswith('27101')) & (col('Sector_prog1').startswith('09'))), 'Social Assistance')
            # Basic Wages computed after computing econ categories
            # Social Benefits (pension contributions)
            .when((col('Item_econ4').startswith('21201')), 'Social Benefits (pension contributions)')
            # Employment Contracts
            .when((col('Sub-Item_econ5').startswith('2211310')| col('Sub-Item_econ5').startswith('2110201')), 'Employment Contracts')
            # Allowances
            .when(((col('Class').startswith('0')) & (col('Item_econ4').startswith('21103'))), 'Allowances')
            # foreign funded cap ex
            .when((col('Chapter_econ2').startswith('31') & (~col('SOF2').startswith('00'))), 'Capital Expenditure (foreign spending)')
            # Basic Services            
            .when(
                ((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('22')) & (
                    (col('Sub-Item_econ5').startswith('2211201')) | 
                    (col('Sub-Item_econ5').startswith('2210201')) |
                    (col('Sub-Item_econ5').startswith('2210102')) |
                    (col('Sub-Item_econ5').startswith('2210101')) |
                    (col('Sub-Item_econ5').startswith('2211015'))
                )), 'Basic Services')
            # Recurrent Maintenance
            .when(col('Item_econ4').startswith('22202'), 'Recurrent Maintenance')
            # Subsidies to Production
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('25'))), 'Subsidies to Production')
            #Pensions
            .when(((~col('Class').startswith('2')) & (col('Item_econ4').startswith('27101'))), 'Pensions')
        ).withColumn('econ',
            # wage bill
            when((
                    ((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('21'))) | 
                    ((col('Item_econ4').startswith('22103') | col('Item_econ4').startswith('22104')  | col('Item_econ4').startswith('22107')) & (col('Class').startswith('0'))) |
                    ((col('Item_econ4').startswith('22103') | col('Item_econ4').startswith('22104')  | col('Item_econ4').startswith('22107')) & (col('Class').startswith('1')))                   
                ), 'Wage bill')
            # cap ex
            .when(col('Chapter_econ2').startswith('31'), 'Capital expenditures')
            # Goods and services
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('22'))), 'Goods and services')
            # subsidies           
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('25'))), 'Subsidies')
            # social benefits
            .when(col("econ_sub").isin("Social Assistance", "Pensions"), 'Social benefits')
            # other grants and transfers category not available
            # interest on debt
            .when(((~col('Class').startswith('2')) & (col('Chapter_econ2').startswith('24'))), 'Interest on debt')
            # other expenses
            .otherwise('Other expenses')
        )
    )
    
@dlt.table(name=f'ken_boost_gold')
def boost_gold():
    return (dlt.read(f'ken_boost_silver')
        .filter(col('year') != 2015) # 2015 data is missing wage amounts for education, exclude entirely for correness sake
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                col('Final_Budget_Approved_Estimate').alias('approved').cast(DoubleType()),
                col('approved').alias('revised'),
                col('`Final_Expenditure_Total_Payment_Comm.`').alias('executed').cast(DoubleType()),
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
