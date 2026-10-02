# Databricks notebook source
import dlt
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Yemen'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name='yem_boost_bronze')
def boost_bronze():
    # One file per year (YYYY.csv) in the layout of the workbook's BOOST tab -- Year, Func, Admin1, Econ1,
    # econ2, Executed -- written by YEM_extract_raw_microdata_excel_to_csv.py from the Ministry of
    # Finance's yearly sector workbooks; a new year's file is picked up by the pattern.
    return (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/????.csv')
    )

# COMMAND ----------

@dlt.table(name='yem_boost_silver')
def boost_silver():
    # Every category below is the workbook's Executed-sheet SUMIFS criterion (the EXP_* code in the comment)
    # applied to the BOOST-tab labels. Every formula is SUM(SUMIFS(Executed, Year, <col>$1, ...)); only the
    # criteria are quoted. Excel text criteria are case-insensitive, so "wage*" is written in the label's
    # own case, 'Wage'. See verification.md for the one overlap found and the decisions left to the expert.
    return (dlt.read('yem_boost_bronze')
        # the raw unit column; Spark column names are case-insensitive, so admin1 below would overwrite it
        .withColumnRenamed('Admin1', 'admin1_tmp')
        .withColumn('year', col('Year').cast(IntegerType())
        # EXP_ECON_TOT_EXP_EXE  Econ1,"<>assets*": Title V (lending, loan repayments) is outside the total and
        # outside every function formula (each carries the same criterion), so the lines are dropped (D1)
        ).filter(~col('Econ1').startswith('Assets')
        # no subnational formula in the workbook: every line is spent by a central spending unit
        ).withColumn('admin0', lit('Central')
        ).withColumn('admin1', lit('Central Scope')
        # the spending unit: ministry, authority, university, fund ("Ministry of Education", "Sana`a University")
        ).withColumn('admin2', col('admin1_tmp')
        ).withColumn('geo1', lit('Central Scope')
        # EXP_ECON_TOT_EXP_FOR_EXE has no formula (every cell "..")
        ).withColumn('is_foreign', lit(None).cast('boolean')
        ).withColumn('func_sub',
            # EXP_FUNC_AGR_EXE  Func,"economic*",Admin1,{"general*","Ministry of Agriculture and Irrigation *"}
            # (the unit's label carries a trailing space, which the criterion's wildcard absorbs)
            when(col('Func').startswith('Economic') & (col('admin1_tmp').startswith('General') | col('admin1_tmp').startswith('Ministry of Agriculture and Irrigation ')), 'Agriculture')
            # EXP_FUNC_TRA_EXE  Admin1,"Ministry of Transportation"
            .when(col('admin1_tmp') == 'Ministry of Transportation', 'Transport')
            # EXP_FUNC_ROA_EXE  Admin1,"Ministry of Public Works and Highways" (the only unit under Func "Population and
            # Community Development", so Roads sits under Housing here, D3)
            .when(col('admin1_tmp') == 'Ministry of Public Works and Highways', 'Roads')
            # EXP_FUNC_ENE_EXE  Admin1,{"Ministry of Electricity and Energy","Ministry of Oil and Minerals"}; its two
            # leaves, Energy (power) row 143 and Energy (oil & gas) row 155, are not emitted (D4)
            .when(col('admin1_tmp').isin('Ministry of Electricity and Energy', 'Ministry of Oil and Minerals'), 'Energy')
            # EXP_FUNC_TEL_EXE  Admin1,"Ministry of Communications and Information Technology"
            .when(col('admin1_tmp') == 'Ministry of Communications and Information Technology', 'Telecom')
        ).withColumn('func',
            # EXP_FUNC_GEN_PUB_SER_EXE  Func,"general p*" + row 26 (EXP_ECON_INT_DEB_EXE  Econ1,"int*"); first, so that an
            # interest line goes nowhere else (the workbook would count it in its own function as well, Q1)
            when(col('Func').startswith('General P') | col('Econ1').startswith('Interest'), 'General public services')
            # EXP_FUNC_DEF_EXE  Func,"def*"
            .when(col('Func').startswith('Def'), 'Defence')
            # EXP_FUNC_PUB_ORD_SAF_EXE  Func,"public s*"
            .when(col('Func').startswith('Public S'), 'Public order and safety')
            # EXP_FUNC_ECO_REL_EXE  Func,"economic*"
            .when(col('Func').startswith('Economic'), 'Economic affairs')
            # EXP_FUNC_ENV_PRO_EXE  Func,"envi*"
            .when(col('Func').startswith('Envi'), 'Environmental protection')
            # EXP_FUNC_HOU_EXE  Func,"Population and Community Development"
            .when(col('Func') == 'Population and Community Development', 'Housing and community amenities')
            # EXP_FUNC_HEA_EXE  Func,"health*"
            .when(col('Func').startswith('Health'), 'Health')
            # EXP_FUNC_REV_CUS_EXC_EXE  Func,"culture*"
            .when(col('Func').startswith('Culture'), 'Recreation, culture and religion')
            # EXP_FUNC_EDU_EXE  Func,"ed*"
            .when(col('Func').startswith('Ed'), 'Education')
            # EXP_FUNC_SOC_PRO_EXE  Func,"social p*"
            .when(col('Func').startswith('Social P'), 'Social protection')
            # the workbook has no residual function: a "Debt" line that is not interest is in no function and stays untagged
        ).withColumn('econ_sub',
            # EXP_ECON_PEN_CON_EXE  econ2,"social c*"
            when(col('econ2').startswith('Social C'), 'Social Benefits (pension contributions)')
            # EXP_ECON_REC_MAI_EXE  Econ1,"<>assets*",econ2,"maint*"
            .when(col('econ2').startswith('Maint'), 'Recurrent Maintenance')
        ).withColumn('econ',
            # EXP_ECON_WAG_BIL_EXE  Econ1,"wage*"
            when(col('Econ1').startswith('Wage'), 'Wage bill')
            # EXP_ECON_CAP_EXP_EXE  Econ1,"acq*"
            .when(col('Econ1').startswith('Acq'), 'Capital expenditures')
            # EXP_ECON_USE_GOO_SER_EXE  Econ1,"use*"
            .when(col('Econ1').startswith('Use'), 'Goods and services')
            # EXP_ECON_SUB_EXE  econ2,"financial*"
            .when(col('econ2').startswith('Financial'), 'Subsidies')
            # EXP_ECON_SOC_BEN_EXE  econ2,"social b*"
            .when(col('econ2').startswith('Social B'), 'Social benefits')
            # EXP_ECON_OTH_GRA_EXE  Econ1,"grants*",econ2,"<>financial*",econ2,"<>social b*"
            .when(col('Econ1').startswith('Grants') & ~col('econ2').startswith('Financial') & ~col('econ2').startswith('Social B'), 'Other grants and transfers')
            # EXP_ECON_INT_DEB_EXE  Econ1,"int*"
            .when(col('Econ1').startswith('Interest'), 'Interest on debt')
            # Other expenses = Total - the seven categories (row 22; the sheet's cell is written as the total itself,
            # verification.md X1). The seven cover every Title I-IV line, so no line reaches here.
            .otherwise('Other expenses')
        )
    )

# COMMAND ----------

@dlt.expect_or_drop("executed_not_null", "executed IS NOT NULL")
@dlt.table(name='yem_boost_gold')
def boost_gold():
    # The BOOST tab carries executed amounts only and the Approved sheet has no formula (every cell ".."),
    # so approved and revised are null (D5).
    return (dlt.read('yem_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                lit(None).cast(DoubleType()).alias('approved'),
                lit(None).cast(DoubleType()).alias('revised'),
                col('Executed').cast(DoubleType()).alias('executed'),
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
