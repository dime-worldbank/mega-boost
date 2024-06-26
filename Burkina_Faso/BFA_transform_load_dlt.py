# Databricks notebook source
from glob import glob
from pyspark.sql.types import StructType
import dlt
from pyspark.sql.functions import col, lower, initcap, trim, regexp_replace, when, lit, substring, expr, coalesce

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Burkina Faso'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'bfa_boost_bronze_1')
def boost_bronze_1():
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(f'{COUNTRY_MICRODATA_DIR}/BOOST.csv'))

    bronze1_selected_columns = ['YEAR','ADMIN1', 'GEO1', 'ECON1', 'ECON2', 'ECON3', 'ECON4', 'FUNCTION1','FUNCTION2', 'SOURCE_FIN1', 'SECTOR2', 'rep_cap', 'APPROVED', 'MODIFIED', 'PAID'] 
    bronze1_dlt = bronze_df.select(*bronze1_selected_columns)
    bronze1_filtered_dlt = bronze1_dlt.na.drop("all")
    bronze1_filtered_dlt = bronze1_filtered_dlt.filter(col("YEAR") < 2017)
    return bronze1_filtered_dlt


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f'bfa_boost_bronze_2')
def boost_bronze_2():
    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(f'{COUNTRY_MICRODATA_DIR}/BOOST_.csv'))

    bronze2_selected_columns = ["YEAR", 'ADMIN1', "GEO1", "ECON1", "ECON2", "ECON3", "FUNCTION1", 'FUNCTION2', 'ZONEFINA', 'PROG1', 'PROG3', 'rep_cap', "APPROVED", "REVISED", "PAID"] 
    bronze2_dlt = bronze_df.select(*bronze2_selected_columns)
    bronze2_filtered_dlt = bronze2_dlt.na.drop("all")
    return bronze2_filtered_dlt

@dlt.table(name=f'bfa_boost_bronze')
def boost_bronze_combined():
    bronze1 = dlt.read(f'bfa_boost_bronze_1')
    bronze2 = dlt.read(f'bfa_boost_bronze_2')
    # Rename specific columns in bronze1 and bronze2
    bronze1 = bronze1.withColumnRenamed(
        "MODIFIED", "REVISED"
        ).withColumn('PROG1', lit(None)
        ).withColumn('PROG3', lit(None)
        ).withColumn("PAID", 
            when(col("PAID").isNull(), 0)
            .otherwise(col("PAID"))
        )
        
    bronze2 = bronze2.withColumn(
        'SECTOR2', lit(None)
        ).withColumn('ECON4', lit(None)
        ).withColumnRenamed("ZONEFINA", "SOURCE_FIN1")

    # Concatenate the two DataFrames
    bronze = bronze2.unionByName(bronze1)
    return bronze

econ4_codes_to_exclude = [
    '63111', '63113', '63114', '63131', '63221', '63411', '63911', '63912', '63919', '63971', '63119',
    '63120', '63122', '63123', '63125', '63132', '63133', '63141', '63922', '63923', '63925', '63929',
    '63972'
]
@dlt.table(name=f'bfa_boost_silver')
def boost_silver():
    bronze = dlt.read(f'bfa_boost_bronze')
    silver_df = bronze.withColumn(
        'adm1_name_tmp',
        when(col("GEO1").isNotNull(),
             trim(initcap(regexp_replace(col("GEO1"), "[0-9\-]", " "))))
    ).withColumn('ECON1', coalesce(col('ECON1'), lit(''))
    ).withColumn('ECON2', coalesce(col('ECON2'), lit(''))
    ).withColumn(
        'is_foreign', (((col('YEAR')<2017) & (~col('SOURCE_FIN1').startswith('1'))) | ((col('Year')>=2017) & (col('SOURCE_FIN1') != 'Financement Exterieur')))
    ).withColumn(
        'admin0', lit('Central')
    ).withColumn(
        'admin1', lit('Central')
    ).withColumn(
        'admin2', col('ADMIN1')
    ).withColumn(
        'geo1',
        when(col('adm1_name_tmp').isin('Central', 'Centrale'), 'Central Scope')
        .when(col('adm1_name_tmp') == 'Region Etrangere', 'Other')
        .when(col('adm1_name_tmp') == 'Est', 'Est Region Burkina Faso')
        .when(col('adm1_name_tmp') == 'Centre Sud', 'Centre Sud Region Burkina Faso')
        .otherwise(col('adm1_name_tmp'))
    ).withColumn(
        'func_sub',
        when(col('FUNCTION2').startswith('033'), 'judiciary')
        .when(col('FUNCTION1').startswith('03') & (~col('FUNCTION2').startswith('033')), 'public safety')
        .when(col('FUNCTION2').startswith('072') | col('FUNCTION2').startswith('074'), 'primary and secondary health')
        .when(col('FUNCTION2').startswith('073'), 'tertiary and quaternary health')
        .when(col('FUNCTION2').startswith('091') | ((col('SECTOR2').startswith('42')) & (col('FUNCTION2').startswith('095') | col('FUNCTION2').startswith('096'))), 'primary education')
        .when((col('YEAR') < 2017) & (col('FUNCTION2').startswith('092')), 'secondary education')
    ).withColumn(
        'func',
        when(col('FUNCTION1').startswith('02'), 'Defence')
        .when(col('func_sub').isin('judiciary', 'public safety'), 'Public order and safety')
        .when(col('FUNCTION1').startswith('04'), 'Economic affairs')
        .when(col('FUNCTION1').startswith('05'), 'Environmental protection')
        .when(col('FUNCTION1').startswith('06'), 'Housing and community amenities')
        .when(col('FUNCTION1').startswith('07'), 'Health')
        .when(col('FUNCTION1').startswith('08'), 'Recreation, culture and religion')
        .when(col('FUNCTION1').startswith('09'), 'Education')
        .when(col('FUNCTION1').startswith('10'), 'Social protection')
        .otherwise('General public services')
    ).withColumn(
        'econ_sub',
        when(((col('YEAR')==2006) & (col('FUNCTION1').startswith('10') & (col('ECON1').startswith('4 ')))), 'social assistance') # different formula for 2006
        .when((((col('YEAR')>2006) & (col('YEAR')<2017)) & 
               (col('FUNCTION1').startswith('10') & col('ECON1').startswith('4 '))), 'social assistance')
        .when(((col('YEAR')>=2017) & (col('FUNCTION1').startswith('10')) & (col('ECON1').startswith('4 '))), 'social assistance') # no formulae for pensions and other social benefits
        .when(((col('YEAR')<2017) & (
            ((~col('SOURCE_FIN1').startswith('1')) & (col('ECON1').startswith('5')) & (~col('ECON2').startswith('66'))) |
            (col('ECON1').startswith('6')) |
            (col('ECON1').startswith('7') & (col('ECON2').startswith('21') | col('ECON2').startswith('22') | col('ECON2').startswith('23'))) |
            (col('ECON1').startswith('7') & (col('ECON4').startswith('62997')) )
            )
        ), 'capital expenditure (foreign spending)')
        .when(((col('YEAR')>=2017) & (col('ECON1').startswith('5')) & (col('SOURCE_FIN1')!= 'Financement Etat')), 'capital expenditure (foreign spending)')
        .when(col('rep_cap').startswith('y'), 'capital maintenance')
        .when(((col('YEAR')<2017) & (col('ECON3').startswith('625') | col('ECON3').startswith('627'))), 'basic services')
        .when(((col('YEAR')>=2017) & (col('ECON3').startswith('605') | col('ECON3').startswith('612'))), 'basic services')
        .when(((col('YEAR')<2017) & (col('ECON3').startswith('623'))), 'employment contracts')
        .when(((col('YEAR')>=2017) & (col('ECON3').startswith('622'))), 'employment contracts')
        .when(((col('YEAR')<2017) & (col('ECON3').startswith('622'))), 'recurrent maintenance')
        .when(((col('YEAR')>=2017) & (col('ECON3').startswith('614'))), 'recurrent maintenance')
        # subsidies to production 
        .when((col('YEAR')>=2017) & ((col('ECON2').startswith('63')) | 
                            (col('PROG3').startswith('1330303') | col('PROG3').startswith('1330313'))), 'subsidies to production')
        .when((col('YEAR')<2017) & (col('ECON2').startswith('63')) & (~col('ECON4').startswith('6322')) &  (~col('ECON4').substr(1, 5).isin(econ4_codes_to_exclude)) , 'subsidies to production') # same as 'Subsidies' in econ
    ).withColumn(
        'econ',
        # interest on debt
        when((((col('YEAR')<2017) & (col('ECON2') =='65 Interets et frais financiers'))) |
              ((col('YEAR')>=2017) & (col('ECON2') == '67 INTERETS ET FRAIS FINANCIERS')),  'Interest on debt')
        # Wage bill
        .when((col('YEAR')<2017) & ((col('ECON1').startswith('2')) | (
            col('ECON4').startswith('63111') |
            col('ECON4').startswith('63112') |
            col('ECON4').startswith('63113') |
            col('ECON4').startswith('63114') |
            col('ECON4').startswith('63131') |
            col('ECON4').startswith('63221') |
            col('ECON4').startswith('63411') |
            col('ECON4').startswith('64312') |
            col('ECON4').startswith('63911') |
            col('ECON4').startswith('63912') |
            col('ECON4').startswith('63919') |
            col('ECON4').startswith('63971') |
            col('ECON4').startswith('64221') |
            col('ECON4').startswith('64231'))), 'Wage bill')
        .when(((col('ECON2') == '66 CHARGES DE PERSONNEL') & (~col('ECON1').startswith('5 ')) & (col('YEAR')>=2017)), 'Wage bill')
        # capital expenditure
        .when(((col('YEAR')<2017) & 
            ((col('ECON1').startswith('5') & (~(col('ECON2')=='66 CHARGES DE PERSONNEL'))) |
            (col('ECON1').startswith('6')) |
            ((col('ECON1') == '7 Comptes speciaux du Tresor') & (col('ECON2').startswith('21') | col('ECON2').startswith('22') | col('ECON2').startswith('23'))) | 
            ((col('ECON1') == '7 Comptes speciaux du Tresor') & col('ECON4').startswith('62997')))
            ), 'Capital expenditures')
        .when(((col('YEAR')>2016) & (col('ECON1').startswith('5'))), 'Capital expenditures')
        # Goods and services
        .when(((col('YEAR') == 2006) & ((col('ECON1').startswith('3') & (col('ECON2').startswith('62') | col('ECON2').startswith('24'))) |
                (col('ECON2').startswith('64') & 
                        (col('ECON4').startswith('64112') |
                        col('ECON4').startswith('64132') |
                        col('ECON4').startswith('64222') |
                        col('ECON4').startswith('64232') |
                        col('ECON4').startswith('64521'))) |
                    ((col('ECON1') == '7 Comptes speciaux du Tresor') & (col('ECON2').startswith('62') | col('ECON2').startswith('24')))
                )), 'Goods and services')
        .when(((col('YEAR')>=2017) & (col('ECON1').startswith('3'))), 'Goods and services')
        .when(((col('YEAR')<2017) & 
               ((col('ECON1').startswith('3') & (col('ECON2').startswith('62') | col('ECON2').startswith('24'))) |
                (col('ECON2').startswith('64') & 
                        (col('ECON4').startswith('64112') |
                        col('ECON4').startswith('64132') |
                        col('ECON4').startswith('64222') |
                        col('ECON4').startswith('64232') |
                        col('ECON4').startswith('64521'))) |
                    ((col('ECON1') == '7 Comptes speciaux du Tresor') & (col('ECON2').startswith('62') | col('ECON2').startswith('24')) & (~col('ECON4').startswith('62997 ')))
                )
            ), 'Goods and services')
        # Social benefits
        .when(col('econ_sub').isin('social assistance', 'pensions', 'other social benefits'), 'Social benefits')
        # subsidies
        .when((col('YEAR')>=2017) & ((col('ECON2').startswith('63')) | 
                            (col('PROG3').startswith('1330303') | col('PROG3').startswith('1330313'))), 'Subsidies')
        .when(((col('YEAR')<2017) & (col('ECON2').startswith('63')) & (~col('ECON4').startswith('6322')) & (~col('ECON4').substr(1, 5).isin(econ4_codes_to_exclude))) , 'Subsidies')
        # other expenses
        .otherwise('Other expenses')
    )
    return silver_df

@dlt.table(name=f'bfa_boost_gold')
def boost_gold():
    silver = dlt.read(f'bfa_boost_silver')
    gold_df = (silver
               .filter(col('ECON1') != '1 Amortissement, charge de la dette et depenses en attenuation des recettes ')
               .withColumn('country_name', lit(COUNTRY))
               .select('country_name',
                       col('YEAR').alias('year').cast('int'),
                       col('APPROVED').alias('approved'),
                       col('REVISED').alias('revised'),
                       col('PAID').alias('executed'),
                       'admin0',
                       'admin1',
                       'admin2',
                       'geo1',
                       'func',
                       'econ_sub',
                       'econ'
                       )
              )
    return gold_df

