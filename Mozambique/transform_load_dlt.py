# Databricks notebook source
import dlt
from pyspark.sql.functions import substring, col, lit, when, trim, regexp_replace 

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Mozambique'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'
AUXILIARY_CSV = f'{WORKSPACE_DIR}/auxiliary_csv/Mozambique/adm5.csv'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.table(name=f'moz_adm5_master_key_bronze')
def adm5_master_key_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(AUXILIARY_CSV)
    )

@dlt.expect_or_drop("exp_type_not_null", "ExpType IS NOT NULL")
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'moz_boost_bronze')
def boost_bronze():
    return (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(COUNTRY_MICRODATA_DIR)
    )

@dlt.table(name=f'moz_boost_silver')
def boost_silver():
    # take the opportunity to make Adm5 consistent - unify with/without accent, upper/lower case
    return (dlt.read(f'moz_boost_bronze')
        .select("*",substring('Adm5', 1, 1).alias("UGB_third"))
        .join(dlt.read(f'moz_adm5_master_key_bronze'), ["UGB_third"], "left")
        .drop('Adm5')
        .select("*", col('Adm51').alias('Adm5'))
        .drop('Adm51', 'UGB_third')
        .withColumn('DotacaoInicial', col('DotacaoInicial').cast('double'))
        .withColumn('DotacaoActualizada', col('DotacaoActualizada').cast('double'))
        .withColumn('Execution', col('Execution').cast('double'))
        .withColumn('geo1',
            when(col("Adm5En") == "Maputo (city)", "Cidade de Maputo")
            .otherwise(
                when(col("Adm5En") == "Central", "Central Scope")
                .otherwise(col("Adm5En")))
        ).withColumn('admin0',
            when(col('Adm5').startswith('A'), 'Central')
            .otherwise('Regional')
        ).withColumn('admin1', col('geo1')
        ).withColumn('admin2',
            trim(regexp_replace(col("Adm2"), '^[0-9\\s]*', ''))
        ).withColumn('is_foreign', ~col('Fund1').startswith('1') # foreign funded expenditure
        ).withColumn('func_sub',
            # Judiciary
            when(col("Func1").startswith('03') & (col('Func2') == '03311 Tribunais') , "Judiciary")
            # Public Safety
            .when(col("Func1").startswith('03'), "Public Safety" ) # important for this to be after Judiciary
            # education expenditure breakdown
            .when((col('Func1').startswith('09') & (
                col('Func2').startswith('09111') | col('Func2').startswith('09121') | col('Func2').startswith('09122') | col('Func2').startswith('09113')
            )), 'Primary Education')
            .when((col('Func1').startswith('09') & (
                col('Func2').startswith('09211') | col('Func2').startswith('09212')
            )), 'Secondary Education')
            .when((col('Func1').startswith('09') & (
                col('Func2').startswith('09411') | col('Func2').startswith('09412') | col('Func2').startswith('09419') | col('Func2').startswith('09431')
            )), 'Tertiary Education')
            # health expenditure breakdown
            .when(col('Func2').startswith('07411'), 'Primary and Secondary Health')
            .when((col('Func2').startswith('07311') | col('Func2').startswith('07321')), 'Tertiary and Quaternary Health')
        ).withColumn('func',
            when(col('Func1').startswith("01"), "General public services")
            .when(col('Func1').startswith("02"), "Defence")
            .when(col("func_sub").isin("Judiciary", "Public Safety") , "Public order and safety")
            .when(col('Func1').startswith("04"), "Economic affairs")
            .when(col('Func1').startswith("05"), "Environmental protection")
            .when(col('Func1').startswith("06"), "Housing and community amenities")
            .when(col('Func1').startswith("07"), "Health")
            .when(col('Func1').startswith("08"), "Recreation, culture and religion")
            .when(col('Func1').startswith("09"), "Education")
            .when(col('Func1').startswith("10"), "Social protection")
        ).withColumn( 'econ_sub',
            # Pensions
            when((col('Econ4').startswith('1431') | col('Econ4').startswith('1432')), 'Pensions')
            # Social Assistance
            .when(((col('Func1').startswith('10')) &
                   (~col('Econ4').startswith('1431')) &
                   (~col('Econ4').startswith('1432'))), 'Social Assistance')
            # wage bill breakdowm missing
            # capital expenditure (foreign funded)
            .when((col('Econ1').startswith('2') & (~col('Fund1').startswith('1'))), 'Capital Expenditure (foreign spending)')
            # Basic Services
            .when((col('Econ5').startswith('121001') | col('Econ5').startswith('122013') | col('Econ5').startswith('122004') | col('Econ5').startswith('121010')), 'Basic Services')
            # Employment Contracts
            .when((col('Econ5').startswith('121014') | col('Econ5').startswith('122015')), 'Employment Contracts')
            # Recurrent Maintenance
            .when((col('Econ5').startswith('121002') |
                   col('Econ5').startswith('122003') |
                   col('Econ5').startswith('122005') |
                   col('Econ5').startswith('122006') |
                   col('Econ5').startswith('122007')), 'Recurrent Maintenance')
            # Subsidies to Production
            .when(col('Econ2').startswith('15'), 'Subsidies to Production') # same as the value for subsidies

        ).withColumn('econ',
            # social benefits
            when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits') # should come before other econ categories
            # cap ex
            .when((col('Econ1').startswith('2')) & (~col('Func1').startswith('10')), 'Capital expenditures')
            # wage bill
            .when((col('Econ2').startswith('11')) & (~col('Func1').startswith('10')), 'Wage bill')
            # goods and services
            .when((col('Econ2').startswith('12')) & (~col('Func1').startswith('10')), 'Goods and services')
            # subsidies
            .when(col('Econ2').startswith('15'), 'Subsidies')
            # interest on debt
            .when(col('Econ2').startswith('13'), 'Interest on debt')
            # other expenses
            .otherwise('Other expenses')
        )
    )
    
@dlt.table(name=f'moz_boost_gold')
def boost_gold():
    return (dlt.read(f'moz_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                col('Year').alias('year'),
                col('DotacaoInicial').alias('approved'),
                col('DotacaoActualizada').alias('revised'),
                col('Execution').alias('executed'),
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
