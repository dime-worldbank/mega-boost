# Databricks notebook source
import dlt
import json
import unicodedata
from pyspark.sql.functions import col, lower, regexp_extract, regexp_replace, when, lit, substring, expr, floor, concat, udf, lpad
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

with open(f"{RAW_INPUT_DIR}/{COUNTRY}/2023/labels_en_v01_overall.json", 'r') as json_file:
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
    file_paths = glob(f"{RAW_COUNTRY_MICRODATA_DIR}/*.csv")
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(file_paths))
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'alb_2022_and_before_boost_bronze')
def boost_bronze():
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Data_Expenditures.csv')
    )


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
        ).withColumn("econ3", when(col("econ3") == 6780, 678).otherwise(col("econ3"))
        ).withColumn("econ1",
            when(col("econ5").isNotNull(), substring(col("econ5").cast("string"), 1, 1).cast("int"))
            .otherwise(substring(col("econ3").cast("string"), 1, 1).cast("int"))
        ).withColumn("econ2",
            when(col("econ5").isNotNull(), substring(col("econ5").cast("string"), 1, 2).cast("int"))
            .otherwise(substring(col("econ3").cast("string"), 1, 2).cast("int"))
        ).withColumn("econ4",
            when(col("econ5").isNotNull(), substring(col("econ5").cast("string"), 1, 4).cast("int"))
        ).filter((col("econ3") != 255) & (col("econ3") >= 230)
        ).filter(~col("econ1").isin([16, 17])
        ).withColumn("econ1",
            when(col("executed").isNull(), substring(col("econ3").cast("string"), 1, 1).cast("int"))
            .otherwise(col("econ1"))
        ).withColumn("econ2",
            when(col("executed").isNull(), substring(col("econ3").cast("string"), 1, 2).cast("int"))
            .otherwise(col("econ2"))
        # functional tagging
        ).withColumn("program1", col("func3")
        ).withColumn("func3", col("func3").cast("double") # substituting with values from func3_n for those where the code is alphanumeric
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
        ).withColumn("func1", lpad(col('func1'), 2, "0")
        ).withColumn("func2",(col("func3") / 100).cast("int")
        ).withColumn("func2", lpad(col('func2'), 3, "0"      )        
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
        ).withColumn("transfer",
            when(
                ((col("econ5") == 6040005) & (col("admin2") == 1)) | 
                col("econ5").isin([6040001, 6040002, 6040006, 6040007, 6040010, 6041100 ]), 1)
            .when(col("econ4") == 6042, 1)
            .when(
                ((col("econ3") == 604) & (col("admin4") == 1010098)) |
                ((col("econ3") == 604) & (col("admin4") == 1025096) & (col("admin3") == 25)) |
                ((col("econ3") == 604) & (col("admin4") == 1010226) & (col("admin3") == 10)), 1)
            .otherwise(lit(0))
        )
    for column_name, mapping in labels.items():
        if column_name in silver_df.columns:
            silver_df = silver_df.withColumn(column_name, replacement_udf(column_name)(col(column_name)))

    silver_df = silver_df.filter(col('transfer')=='Excluding transfers'
        ).withColumn('is_foreign', col('fin_source').startswith('2')
        ).withColumn('admin0', 
            when(col('admin2').startswith('00') | col('admin2').startswith('999'), 'Central')
            .otherwise('Regional')    
        ).withColumn('admin1_tmp',
            when(col('counties')=='Central', 'Central Scope')
            .otherwise(col('counties'))
        ).withColumn('admin2_tmp',
            when(col('counties')=='Central', col('admin3'))
            .otherwise(col('counties'))
        ).withColumn('geo1', col('admin1_tmp')
        ).withColumn('func_sub',
            # spending in judiciary
            when(col('func2').startswith('033'), 'judiciary')
            # public safety
            .when(col('func2').substr(1,3).isin(['031', '034', '035']), 'public safety')
            # spending in energy
            .when(col('func2').startswith('043'), 'Energy')
            # primary and secondary health
            .when(col('func2').startswith('072') | col('func2').startswith('074'), 'primary and secondary health')
            # tertitaey and quaternary health
            .when(col('func2').startswith('073'), 'tertiary and quaternary health')
            # primary education
            .when(col('func1').startswith('09') & col('func2').startswith('091'), 'primary education')
            # secondary education
            .when(col('func1').startswith('09') & col('func2').startswith('092'), 'secondary education')
            # tertiary education
            .when(col('func1').startswith('09') & col('func2').startswith('094'), 'tertiary education')
        ).withColumn('func',
            # public order and safety
            when(col('func_sub').isin('judiciary', 'public safety'), 'Public order and safety')
            # defense
            .when(col('func1').startswith('02'), 'Defence')
            # economic relations
            .when(col('func1').startswith('04'), 'Economic affairs')
            # environmental protection
            .when(col('func1').startswith('05'), 'Environmental protection')
            # housing
            .when(col('func1').startswith('06'), 'Housing and community amenities')
            # health
            .when(col('func1').startswith('07'), 'Health')
            # recreation, culture, religion
            .when(col('func1').startswith('08'), 'Recreation, culture and religion')
            # education
            .when(col('func1').startswith('09'), 'Education')
            # social protection 
            .when(col('func1').startswith('10'), 'Social protection')
            # general public services
            .otherwise('General public services')
        ).withColumn('econ_sub',
            # allowances
            when((col('econ3').startswith('600') & 
                    col('econ5').substr(1, 7).isin(['6001005', '6001003', '6001006', '6001009', '6001099', '6001008', '6001014', '6001007', '6001012', '6001004'])), 'allowances')
            # basic wages
            .when(col('econ3').startswith('600') | col('econ3').startswith('601'), 'basic wages')
            # pension contributions
            .when(col('econ3').startswith('601'), 'social benefits (pension contributions)') # note this will be zero since it is subsumed into above category
            # capital expenditures (foreign funded)
            .when(col('is_foreign') & col('exp_type').startswith('3'), 'capital expenditure (foreign funded)')
            # no entry for capital maintenance
            # goods and services (basic services)
            .when(col('econ4').startswith('6022') | col('econ4').startswith('6026'), 'basic services')
            # no entry for employment contracts
            # recurrent maintenance
            .when(col('econ4').startswith('6025'), 'recurrent maintenance')
            # subsidies to production
            .when(col('econ3').startswith('603'), 'subsidies to production')
            # social assistance
            .when(col('econ3').startswith('606') & col('func2').startswith('104'), 'social assistance')
            # pensions
            .when(col('econ3').startswith('606') & col('func2').startswith('102'), 'pensions')
            # other social benefits
            .when(col('econ3').startswith('606') & col('func2').startswith('10'), 'other social benefits') # should come after social assistance and pensions
        ).withColumn('econ',         
            # wage bill
            when(col('econ3').startswith('601') | col('econ3').startswith('600'), 'Wage bill')
            # capital expenditure
            .when(col('exp_type').startswith('3'), 'Capital expenditures')
            # goods and services
            .when(col('econ3').startswith('602'), 'Goods and services')
            # subsidies
            .when(col('econ3').startswith('603'), 'Subsidies')
            # social benefits
            .when(col('econ3').startswith('606') & col('func2').startswith('10'), 'Social benefits')
            # other grants and transfers
            .when(col('econ3').startswith('604') | col('econ3').startswith('605'), 'Other grants and transfers')
            # interest on debt
            .when(col('econ2').startswith('65') | col('econ2').startswith('66'), 'Interest on debt')
            # other expenses
            .otherwise('Other expenses')
        )
    return silver_df


@dlt.table(name=f'alb_2022_and_before_boost_silver')
def boost_silver():
    return (dlt.read(f'alb_2022_and_before_boost_bronze')          
            .filter(col('transfer') == 'Excluding transfers'
            ).withColumn('is_foreign', col('fin_source').startswith('2')
            ).withColumn('admin0', 
                when(col('admin2').startswith('00') | col('admin2').startswith('999'), 'Central')
                .otherwise('Regional')    
            ).withColumn('admin1_tmp',
                        when(col('counties')=='Central', 'Central Scope')
                        .otherwise(col('counties'))
            ).withColumn('admin2_tmp',
                        when(col('counties')=='Central', col('admin3'))
                        .otherwise(col('counties'))
            ).withColumn('geo1', col('admin1_tmp')
            ).withColumn('func_sub',
                        # spending in judiciary
                        when(col('func2').startswith('033'), 'judiciary')
                        # public safety
                        .when(col('func2').substr(1,3).isin(['031', '034', '035']), 'public safety')
                        # spending in energy
                        .when(col('func2').startswith('043'), 'Energy')
                        # primary and secondary health
                        .when(col('func2').startswith('072') | col('func2').startswith('074'), 'primary and secondary health')
                        # tertitaey and quaternary health
                        .when(col('func2').startswith('073'), 'tertiary and quaternary health')
                        # primary education
                        .when(col('func1').startswith('09') & col('func2').startswith('091'), 'primary education')
                        # secondary education
                        .when(col('func1').startswith('09') & col('func2').startswith('092'), 'secondary education')
                        # tertiary education
                        .when(col('func1').startswith('09') & col('func2').startswith('094'), 'tertiary education')
            ).withColumn('func',
                        # public order and safety
                        when(col('func_sub').isin('judiciary', 'public safety'), 'Public order and safety')
                        # defense
                        .when(col('func1').startswith('02'), 'Defence')
                        # economic relations
                        .when(col('func1').startswith('04'), 'Economic affairs')
                        # environmental protection
                        .when(col('func1').startswith('05'), 'Environmental protection')
                        # housing
                        .when(col('func1').startswith('06'), 'Housing and community amenities')
                        # health
                        .when(col('func1').startswith('07'), 'Health')
                        # recreation, culture, religion
                        .when(col('func1').startswith('08'), 'Recreation, culture and religion')
                        # education
                        .when(col('func1').startswith('09'), 'Education')
                        # social protection 
                        .when(col('func1').startswith('10'), 'Social protection')
                        # general public services
                        .otherwise('General public services')
            ).withColumn('econ_sub',
                        # allowances
                        when((col('econ3').startswith('600') & 
                               col('econ5').substr(1, 7).isin(['6001005', '6001003', '6001006', '6001009', '6001099', '6001008', '6001014', '6001007', '6001012', '6001004'])), 'allowances')
                        # basic wages
                        .when(col('econ3').startswith('600') | col('econ3').startswith('601'), 'basic wages')
                        # pension contributions
                        .when(col('econ3').startswith('601'), 'social benefits (pension contributions)') # note this will be zero since it is subsumed into above category
                        # capital expenditures (foreign funded)
                        .when(col('is_foreign') & col('exp_type').startswith('3'), 'capital expenditure (foreign funded)')
                        # no entry for capital maintenance
                        # goods and services (basic services)
                        .when(col('econ4').startswith('6022') | col('econ4').startswith('6026'), 'basic services')
                        # no entry for employment contracts
                        # recurrent maintenance
                        .when(col('econ4').startswith('6025'), 'recurrent maintenance')
                        # subsidies to production
                        .when(col('econ3').startswith('603'), 'subsidies to production')
                        # social assistance
                        .when(col('econ3').startswith('606') & col('func2').startswith('104'), 'social assistance')
                        # pensions
                        .when(col('econ3').startswith('606') & col('func2').startswith('102'), 'pensions')
                        # other social benefits
                        .when(col('econ3').startswith('606') & col('func2').startswith('10'), 'other social benefits') # should come after social assistance and pensions
            ).withColumn('econ',         
                        # wage bill
                        when(col('econ3').startswith('601') | col('econ3').startswith('600'), 'Wage bill')
                        # capital expenditure
                        .when(col('exp_type').startswith('3'), 'Capital expenditures')
                        # goods and services
                        .when(col('econ3').startswith('602'), 'Goods and services')
                        # subsidies
                        .when(col('econ3').startswith('603'), 'Subsidies')
                        # social benefits
                        .when(col('econ3').startswith('606') & col('func2').startswith('10'), 'Social benefits')
                        # other grants and transfers
                        .when(col('econ3').startswith('604') | col('econ3').startswith('605'), 'Other grants and transfers')
                        # interest on debt
                        .when(col('econ2').startswith('65') | col('econ2').startswith('66'), 'Interest on debt')
                        # other expenses
                        .otherwise('Other expenses')
                        )
            )
    
@dlt.table(name=f'alb_2022_and_before_boost_gold')
def alb_2022_and_before_boost_gold():
    return (dlt.read(f'alb_2022_and_before_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                'revised',
                'executed',
                'is_foreign',
                'geo1',
                'admin0',
                col('admin1_tmp').alias('admin1'),
                col('admin2_tmp').alias('admin2'),
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )


@dlt.table(name=f'alb_2023_onward_boost_gold')
def alb_2023_onward_boost_gold():
    return (dlt.read(f'alb_2023_onward_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                'revised',
                'executed',
                'is_foreign',
                'geo1',
                'admin0',
                col('admin1_tmp').alias('admin1'),
                col('admin2_tmp').alias('admin2'),
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

@dlt.table(name="alb_boost_gold")
def alb_boost_gold():
    df_before_2023 = dlt.read("alb_2022_and_before_boost_gold")
    df_from_2023 = dlt.read("alb_2023_onward_boost_gold")

    return df_before_2023.unionByName(df_from_2023)

           
