# Databricks notebook source
import dlt
from pyspark.sql.functions import col, lower, when, lit, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Bulgaria'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

# EXP_ECON_TOT_EXP_FOR_EXE: source,{"5 Other International Programmes","7 Other European Funds",
# "8 EU Agricultural Fund","9 EU Cohesion and Structural Funds"} -- exact labels (Excel text criteria are
# case-insensitive); a prefix match would catch the mislabelled "5 State budget" lines of 2023 (D4).
FOREIGN_SOURCES = ['5 other international programmes', '7 other european funds',
                   '8 eu agricultural fund', '9 eu cohesion and structural funds']

# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name='bgr_boost_bronze')
def boost_bronze():
    # One file per year (YYYY.csv) from the rebuilds of the Ministry of Finance extracts
    # (BGR_extract_raw_microdata_txt_to_csv_2005_2019.py and ..._2020_onward.py): the same 15 columns and
    # labels; a new year's file is picked up by the pattern.
    return (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/????.csv')
    )

# COMMAND ----------

@dlt.table(name='bgr_boost_silver')
def boost_silver():
    # The rebuilds carry the signed amounts of the extracts (the workbook's Expenditure sheet, no longer read,
    # held paragraph 19 in absolute value for 2014-2019 and started in 2006; see README.md, "Verification").
    # Every category below is the workbook's SUMIFS criterion (the EXP_* code in the comment) on the labels,
    # which all open with the code: "2 Local", "2.1 Defence", "10.20 Expenses for external services".
    return (dlt.read('bgr_boost_bronze')
        .withColumn('year', col('year').cast(IntegerType())
        ).withColumn('adjusted', col('adjusted').cast(DoubleType())
        ).withColumn('executed', col('executed').cast(DoubleType())
        # blank labels become '' so that a negated startswith is never NULL, which would drop the line
        # out of every category into the residual one
        ).na.fill('', subset=['admin1', 'admin2', 'admin3', 'func1', 'func2', 'func3', 'econ1', 'econ2', 'fin_source1', 'exp_type']
        # "2 Local" = municipalities (EXP_ECON_SBN_TOT_SPE_EXE: admin1,"2 Local"); "1 Central" and "3 Other"
        # (the social security funds; the special spending units from 2021) are central government
        ).withColumn('admin0',
            when(col('admin1').startswith('2 '), 'Regional')
            .otherwise('Central')
        # the district (oblast) of municipal spending, from the unit type "66 Plovdiv region (oblast)
        # municipalities" / "72 Sofia city (capital municipality and districts)"
        ).withColumn('admin1',
            when(col('admin0') == 'Central', 'Central Scope')
            .otherwise(regexp_replace(col('admin2'), r'^\d+\s+|\s+region \(oblast\) municipalities$|\s+\(capital municipality and districts\)$', ''))
        # the budget unit: "1600 Ministry of Health", "5103 Municipality of Blagoevgrad"
        ).withColumn('admin2', regexp_replace(col('admin3'), r'^\d+\s+', '')
        ).withColumn('geo1', col('admin1')
        ).withColumn('is_foreign', lower(col('fin_source1')).isin(FOREIGN_SOURCES)
        ).withColumn('func_sub',
            # EXP_FUNC_JUD_EXE  func2,"2.3 Juridical authority"
            when(col('func2').startswith('2.3 '), 'Judiciary')
            # EXP_FUNC_PUB_SAF_EXE  func2,{"2.2 *","2.4*","2.5*"}
            .when(col('func2').rlike('^2\\.(2|4|5) '), 'Public Safety')
            # EXP_FUNC_AGR_EXE  func2,"8.2 Agriculture, forestry, fishery and hunting"
            .when(col('func2').startswith('8.2 '), 'Agriculture')
            # EXP_FUNC_ROA_EXE  road,"y": the workbook's flag is its lookup "road from func3", activities 831-834
            # and 849 (the 388 culture lines it flagged by hand in 2023 are not reproduced)
            .when(col('func3').rlike('^(831|832|833|834|849) '), 'Roads')
            # EXP_FUNC_RAI_EXE  func3,"835 Activities related to railway transport"
            .when(col('func3').startswith('835 '), 'Railroads')
            # EXP_FUNC_WAT_TRA_EXE  func3,"837 Activities related to water transport"
            .when(col('func3').startswith('837 '), 'Water Transport')
            # EXP_FUNC_AIR_TRA_EXE  func3,"836 Activities related to air transport" (2020 blanked in the workbook, X7)
            .when(col('func3').startswith('836 '), 'Air Transport')
            # EXP_FUNC_ENE_EXE  func2,"8.1 Mining, fuel and energy",func3,"<>808 Other mining activities"
            .when(col('func2').startswith('8.1 ') & ~col('func3').startswith('808 '), 'Energy')
            # EXP_FUNC_TEL_EXE  func3,{"838 Management, control and regulation of communications activities",
            # "839 Post and communications","083 Transport and communications (unclassified)"}
            .when(col('func3').rlike('^(838|839|083) '), 'Telecom')
            # EXP_FUNC_WAT_SAN_EXE  func3,{"603 Sewerage","626 Purification of wastewater from settlements"}
            .when(col('func3').rlike('^(603|626) '), 'Water Supply')
            # EXP_FUNC_PRI_SEC_EDU_EXE  func1,"3 Education",func3,"322 Comprehensive schools"
            .when(col('func1').startswith('3 ') & col('func3').startswith('322 '), 'Primary and Secondary education')
            # EXP_FUNC_TER_EDU_EXE  func1,"3 Education",func3,"341 Academies, universities and tertiary schools"
            .when(col('func1').startswith('3 ') & col('func3').startswith('341 '), 'Tertiary Education')
        ).withColumn('func',
            # EXP_FUNC_DEF_EXE  func2,"2.1 Defence"
            when(col('func2').startswith('2.1 '), 'Defence')
            # EXP_FUNC_PUB_ORD_SAF_EXE  func2,{"2.2 *","2.4*","2.5*","2.3*"}
            .when(col('func_sub').isin('Judiciary', 'Public Safety'), 'Public order and safety')
            # EXP_FUNC_ECO_REL_EXE  func1,"8 Economic*"
            .when(col('func1').startswith('8 '), 'Economic affairs')
            # EXP_FUNC_ENV_PRO_EXE  func2,"6.2 Environment"; gated on func1 so that the two zero-value 2015-16 lines
            # with func1 "7 Recreation" and func2 "6.2" go to Recreation only (the workbook counts them twice)
            .when(col('func2').startswith('6.2 ') & col('func1').startswith('6 '), 'Environmental protection')
            # EXP_FUNC_HOU_EXE  func2,"6.1 Housing*"
            .when(col('func2').startswith('6.1 '), 'Housing and community amenities')
            # EXP_FUNC_HEA_EXE  func1,"4 Healthcare"
            .when(col('func1').startswith('4 '), 'Health')
            # EXP_FUNC_REV_CUS_EXC_EXE  func1,"7 Recreation*"
            .when(col('func1').startswith('7 '), 'Recreation, culture and religion')
            # EXP_FUNC_EDU_EXE  func1,"3 Education"
            .when(col('func1').startswith('3 '), 'Education')
            # EXP_FUNC_SOC_PRO_EXE  func1,"5 social*"
            .when(col('func1').startswith('5 '), 'Social protection')
            # General public services = Total - the nine sectors (row 24)
            .otherwise('General public services')
        ).withColumn('econ_sub',
            # EXP_ECON_SOC_ASS_EXE  func1,"5 Social protection",econ1,"42 Current transfers*"
            when(col('func1').startswith('5 ') & col('econ1').startswith('42 '), 'Social Assistance')
            # EXP_ECON_SOC_BEN_PEN_EXE  econ1,"41 Pensions"
            .when(col('econ1').startswith('41 '), 'Pensions')
            # EXP_ECON_PEN_CON_EXE  econ1,"05 Compulsory employer social security contributions"
            .when(col('econ1').startswith('05 '), 'Social Benefits (pension contributions)')
            # EXP_ECON_CAP_MAI_EXE  econ2,"51.00 Capital repair of fixed tangible assets"
            .when(col('econ2').startswith('51.00 '), 'Capital Maintenance')
            # EXP_ECON_GOO_SER_BAS_SER_EXE  econ2,{"10.11 Food","10.16 Water, fuels and energy"}
            .when(col('econ2').rlike('^10\\.(11|16) '), 'Basic Services')
            # EXP_ECON_GOO_SER_EMP_CON_EXE  econ2,"10.20 Expenses for external services"
            .when(col('econ2').startswith('10.20 '), 'Employment Contracts')
            # EXP_ECON_REC_MAI_EXE  econ2,"10.30 Current repairs expenses"
            .when(col('econ2').startswith('10.30 '), 'Recurrent Maintenance')
        ).withColumn('econ',
            # EXP_ECON_INT_DEB_EXE  interest,"y": the workbook's flag is its lookup "interest from econ1", the
            # sub-paragraphs of paragraphs 21-29 (the 30 lines it flagged by hand in 2023 are not reproduced);
            # first, so that an interest line goes nowhere else (the workbook counted such lines twice, Q1)
            when(col('econ1').rlike('^(21|22|25|26|27|28|29) '), 'Interest on debt')
            # EXP_ECON_WAG_BIL_EXE  exp_type,"1 Personnel"
            .when(col('exp_type').startswith('1 '), 'Wage bill')
            # EXP_ECON_CAP_EXP_EXE  exp_type,"3 Capital"
            .when(col('exp_type').startswith('3 '), 'Capital expenditures')
            # EXP_ECON_USE_GOO_SER_EXE  econ1,"10 Maintenance"
            .when(col('econ1').startswith('10 '), 'Goods and services')
            # EXP_ECON_SUB_EXE  econ1,{"43*","44*","45*"}
            .when(col('econ1').rlike('^(43|44|45) '), 'Subsidies')
            # EXP_ECON_SOC_BEN_EXE  = Social assistance + Pensions (row 20, Other social benefits, is empty)
            .when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits')
            # EXP_ECON_OTH_GRA_EXE  econ1,{"39*","42 Current transfers*"},func1,"<>5 Social protection"
            .when(col('econ1').rlike('^(39|42) ') & ~col('func1').startswith('5 '), 'Other grants and transfers')
            # Other expenses = Total - the seven categories (row 22)
            .otherwise('Other expenses')
        )
    )

# COMMAND ----------

@dlt.expect_or_drop("executed_or_approved", "executed IS NOT NULL OR approved IS NOT NULL")
@dlt.table(name='bgr_boost_gold')
def boost_gold():
    # `adjusted` is the adjusted budget, the column the workbook's `Approved` sheet sums (named range
    # `approved`); there is no separate revised budget, so revised = approved.
    return (dlt.read('bgr_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                col('adjusted').alias('approved'),
                col('adjusted').alias('revised'),
                'executed',
                'admin0',
                'admin1',
                'admin2',
                'geo1',
                'is_foreign',
                'func',
                'func_sub',
                'econ',
                'econ_sub')
    )
