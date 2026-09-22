# Databricks notebook source

import dlt
from pyspark.sql.functions import col, lower, when, lit, coalesce
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Bulgaria'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {"header": "true", "multiline": "true", "quote": '"', "escape": '"'}

# EXP_ECON_TOT_EXP_FOR_EXE: source,{"5 Other International Programmes","7 Other European Funds",
# "8 EU Agricultural Fund","9 EU Cohesion and Structural Funds"} -- exact labels (Excel text criteria are
# case-insensitive); a prefix match would catch the mislabelled "5 State budget" lines of 2023 (D4).
FOREIGN_SOURCES = ['5 other international programmes', '7 other european funds',
                   '8 eu agricultural fund', '9 eu cohesion and structural funds']


def classify(categories, residual=None):
    """[(predicate, name), ...] -> CASE column; `residual` (or NULL) where no predicate matches."""
    expr = None
    for pred, name in categories:
        expr = when(pred, name) if expr is None else expr.when(pred, name)
    return expr if residual is None else expr.otherwise(residual)


# COMMAND ----------

@dlt.table(name='bgr_boost_bronze')
def boost_bronze():
    return (spark.read.format("csv").options(**CSV_READ_OPTIONS).option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Expenditure.csv'))


@dlt.table(name='bgr_boost_bronze_raw_2005_2019')
def boost_bronze_raw_2005_2019():
    # The 2005-2019 rebuild from the Ministry of Finance extracts (BGR_extract_raw_microdata_txt_to_csv_2005_2019.py),
    # labelled like the workbook. Only its paragraph 19.01 lines of 2014-2019 are used (boost_silver).
    return (spark.read.format("csv").options(**CSV_READ_OPTIONS).option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/BGR_expenditures_2005-2019.csv'))


# COMMAND ----------

@dlt.table(name='bgr_boost_silver')
def boost_silver():
    df = (dlt.read('bgr_boost_bronze')
          .withColumn('year', col('year').cast(IntegerType()))
          .filter(col('year').isNotNull()))

    # --- Paragraph 19.01 "Payment of state taxes, penalties and administrative sanctions" carries negative amounts
    #     (refunds) in the Ministry of Finance extracts. The workbook turned them positive in 2014-2019 (NOTE sheet:
    #     'some negative values for econ1 "19 paid taxes" were turned positive'), line by line in 2016-2018 and on
    #     the aggregated lines of 2014, 2015 and 2019, so its executed totals overstate spending by twice the
    #     negatives (39 to 809 million BGN a year). The workbook's 19.01 lines of those years are dropped and the
    #     raw rebuild's lines take their place, with the workbook's helper flags set the way its NOTE sheet defines
    #     them: road = activities 831-834 and 849; interest = paragraphs 21-29, never 19. ---
    taxes_flipped = col('econ2').startswith('19.01') & col('year').between(2014, 2019)
    raw_taxes = (dlt.read('bgr_boost_bronze_raw_2005_2019')
                 .withColumn('year', col('year').cast(IntegerType()))
                 .filter(taxes_flipped)
                 .withColumn('adjusted', col('adjusted').cast(DoubleType()))
                 .withColumn('executed', col('executed').cast(DoubleType()))
                 .withColumn('roads', when(col('func3').rlike('^(831|832|833|834|849) '), 'y'))
                 .withColumn('Interest', lit(None).cast('string'))
                 .select(df.columns))
    df = df.filter(~taxes_flipped).unionByName(raw_taxes)

    # Blank labels/flags become '' so that `~col.startswith(...)` is never NULL (a NULL would silently
    # drop the row out of every `... & ~...` predicate into the residual category).
    for c in ['admin1', 'func1', 'func2', 'func3', 'econ1', 'econ2', 'fin_source1', 'exp_type', 'roads', 'Interest']:
        df = df.withColumn(c, coalesce(col(c).cast('string'), lit('')))

    # --- admin / geo ---
    # "2 Local" = municipalities (EXP_ECON_SBN_TOT_SPE_EXE: admin1,"2 Local"); "1 Central" and "3 Other"
    # (the social security funds) are central government. The Expenditure sheet has no region or ministry
    # column, so admin1/geo1 is null for Local lines and admin2 is null everywhere (verification.md D6).
    df = (df
          .withColumn('admin0', when(col('admin1').startswith('2 '), 'Regional').otherwise('Central'))
          .withColumn('admin1', when(col('admin0') == 'Central', 'Central Scope'))
          .withColumn('admin2', lit(None).cast('string'))
          .withColumn('geo0', col('admin0'))
          .withColumn('geo1', col('admin1'))
          .withColumn('is_foreign', lower(col('fin_source1')).isin(FOREIGN_SOURCES)))

    # --- workbook criteria shared by several categories ---
    interest = lower(col('Interest')) == 'y'                                # EXP_ECON_INT_DEB_EXE  interest,"y"
    social_protection = col('func1').startswith('5 ')                       # EXP_FUNC_SOC_PRO_EXE  func1,"5 social*"
    social_assistance = social_protection & col('econ1').startswith('42 ')  # EXP_ECON_SOC_ASS_EXE  func1,"5 Social protection",econ1,"42 Current transfers*"
    pensions = col('econ1').startswith('41 ')                               # EXP_ECON_SOC_BEN_PEN_EXE  econ1,"41 Pensions"
    roads = lower(col('roads')) == 'y'                                      # EXP_FUNC_ROA_EXE  road,"y"

    # --- econ: the seven workbook categories; Other expenses = Total - the seven (row 22). The Interest
    #     flag is the only within-econ overlap (30 lines, 2023): the workbook counts them twice, here
    #     Interest owns them and `~interest` sits on the other six (verification.md Q1). ---
    econ_cats = [
        (interest, 'Interest on debt'),
        (col('exp_type').startswith('1 ') & ~interest, 'Wage bill'),              # EXP_ECON_WAG_BIL_EXE  exp_type,"1 Personnel"
        (col('exp_type').startswith('3 ') & ~interest, 'Capital expenditures'),   # EXP_ECON_CAP_EXP_EXE  exp_type,"3 Capital"
        (col('econ1').startswith('10 ') & ~interest, 'Goods and services'),       # EXP_ECON_USE_GOO_SER_EXE  econ1,"10 Maintenance"
        (col('econ1').rlike('^(43|44|45) ') & ~interest, 'Subsidies'),            # EXP_ECON_SUB_EXE  econ1,{"43*","44*","45*"}
        ((social_assistance | pensions) & ~interest, 'Social benefits'),          # EXP_ECON_SOC_BEN_EXE  SUM(rows 18:20) = Social assistance + Pensions (row 20 Other social benefits is empty)
        (col('econ1').rlike('^(39|42) ') & ~social_protection & ~interest,        # EXP_ECON_OTH_GRA_EXE  econ1,{"39*","42 Current transfers*"},func1,"<>5 Social protection"
         'Other grants and transfers'),
    ]

    # --- econ_sub: the workbook's sub-rows as written (disjoint econ1 / econ2 codes, no interest
    #     criterion; null where the workbook defines none). Names as in quality/transform_load_dlt.py. ---
    econ_sub_cats = [
        (social_assistance, 'Social Assistance'),
        (pensions, 'Pensions'),
        (col('econ1').startswith('05 '), 'Social Benefits (pension contributions)'),  # EXP_ECON_PEN_CON_EXE  econ1,"05*" / "05 Compulsory employer social security contributions"
        (col('econ2').startswith('51.00 '), 'Capital Maintenance'),                   # EXP_ECON_CAP_MAI_EXE  econ2,"51.00 Capital repair of fixed tangible assets"
        (col('econ2').rlike('^10\\.(11|16) '), 'Basic Services'),                     # EXP_ECON_GOO_SER_BAS_SER_EXE  econ2,{"10.11 Food","10.16 Water, fuels and energy"}
        (col('econ2').startswith('10.20 '), 'Employment Contracts'),                  # EXP_ECON_GOO_SER_EMP_CON_EXE  econ2,"10.20 Expenses for external services"
        (col('econ2').startswith('10.30 '), 'Recurrent Maintenance'),                 # EXP_ECON_REC_MAI_EXE  econ2,"10.30 Current repairs expenses"
    ]

    # --- func: the nine workbook sectors; General public services = Total - the nine (row 24). The
    #     only overlap is two zero-value 2015-16 lines with func1 "7 Recreation" and func2 "6.2
    #     Environment": the workbook counts them in both, here Recreation owns them and Environmental
    #     protection is gated on its func1 (verification.md, func pair). ---
    func_cats = [
        (col('func2').startswith('2.1 '), 'Defence'),                                     # EXP_FUNC_DEF_EXE  func2,"2.1 Defence"
        (col('func2').rlike('^2\\.(2|3|4|5) '), 'Public order and safety'),               # EXP_FUNC_PUB_ORD_SAF_EXE  func2,{"2.2 *","2.4*","2.5*","2.3*"}
        (col('func1').startswith('8 '), 'Economic affairs'),                              # EXP_FUNC_ECO_REL_EXE  func1,"8 Economic*"
        (col('func2').startswith('6.2 ') & col('func1').startswith('6 '), 'Environmental protection'),  # EXP_FUNC_ENV_PRO_EXE  func2,"6.2 Environment"
        (col('func2').startswith('6.1 '), 'Housing and community amenities'),             # EXP_FUNC_HOU_EXE  func2,"6.1 Housing*"
        (col('func1').startswith('4 '), 'Health'),                                        # EXP_FUNC_HEA_EXE  func1,"4 Healthcare"
        (col('func1').startswith('7 '), 'Recreation, culture and religion'),              # EXP_FUNC_REV_CUS_EXC_EXE  func1,"7 Recreation*"
        (col('func1').startswith('3 '), 'Education'),                                     # EXP_FUNC_EDU_EXE  func1,"3 Education"
        (social_protection, 'Social protection'),                                         # EXP_FUNC_SOC_PRO_EXE  func1,"5 social*"
    ]

    # --- func_sub: the workbook's leaves as written (disjoint on the microdata; Transport, row 63, is
    #     Roads + Railroads + Water + Air transport). Roads follows the `road` flag, so the 388 flagged
    #     culture lines of 2023 carry func Recreation with func_sub Roads, as the workbook counts them. ---
    func_sub_cats = [
        (col('func2').startswith('2.3 '), 'Judiciary'),                                   # EXP_FUNC_JUD_EXE  func2,{"2.3*"} / {"2.3 Juridical authority"}
        (col('func2').rlike('^2\\.(2|4|5) '), 'Public Safety'),                           # EXP_FUNC_PUB_SAF_EXE  func2,{"2.2 *","2.4*","2.5*"}
        (col('func2').startswith('8.2 '), 'Agriculture'),                                 # EXP_FUNC_AGR_EXE  func2,"8.2 Agriculture, forestry, fishery and hunting"
        (roads, 'Roads'),                                                                 # EXP_FUNC_ROA_EXE  road,"y"
        (col('func3').startswith('835 '), 'Railroads'),                                   # EXP_FUNC_RAI_EXE  func3,"835 Activities related to railway transport"
        (col('func3').startswith('837 '), 'Water Transport'),                             # EXP_FUNC_WAT_TRA_EXE  func3,"837 Activities related to water transport"
        (col('func3').startswith('836 '), 'Air Transport'),                               # EXP_FUNC_AIR_TRA_EXE  func3,"836 Activities related to air transport" (2020 blanked in the workbook, X7)
        (col('func2').startswith('8.1 ') & ~col('func3').startswith('808 '), 'Energy'),   # EXP_FUNC_ENE_EXE  func2,"8.1 Mining, fuel and energy",func3,"<>808 Other mining activities"
        (col('func3').rlike('^(838|839|083) '), 'Telecom'),                               # EXP_FUNC_TEL_EXE  func3,{"838 Management, control and regulation of communications activities","839 Post and communications","083 Transport and communications (unclassified)"}
        (col('func3').rlike('^(603|626) '), 'Water Supply'),                              # EXP_FUNC_WAT_SAN_EXE  func3,{"603 Sewerage","626 Purification of wastewater from settlements"}
        (col('func1').startswith('3 ') & col('func3').startswith('322 '), 'Primary and Secondary education'),  # EXP_FUNC_PRI_SEC_EDU_EXE  func1,"3 Education",func3,"322 Comprehensive schools"
        (col('func1').startswith('3 ') & col('func3').startswith('341 '), 'Tertiary Education'),               # EXP_FUNC_TER_EDU_EXE  func1,"3 Education",func3,"341 Academies, universities and tertiary schools"
    ]

    return (df
            .withColumn('econ', classify(econ_cats, 'Other expenses'))
            .withColumn('econ_sub', classify(econ_sub_cats))
            .withColumn('func', classify(func_cats, 'General public services'))
            .withColumn('func_sub', classify(func_sub_cats)))


# COMMAND ----------

@dlt.table(name='bgr_boost_gold')
@dlt.expect_or_drop("executed_or_approved", "executed IS NOT NULL OR approved IS NOT NULL")
def boost_gold():
    # `adjusted` is the adjusted budget -- the column the workbook's `Approved` sheet sums (named range
    # `approved`); there is no separate revised budget, so revised = approved.
    return (dlt.read('bgr_boost_silver')
            .withColumn('country_name', lit(COUNTRY))
            .withColumn('approved', col('adjusted').cast(DoubleType()))
            .withColumn('revised', col('adjusted').cast(DoubleType()))
            .withColumn('executed', col('executed').cast(DoubleType()))
            .select('country_name', 'year', 'admin0', 'admin1', 'admin2', 'geo0', 'geo1',
                    'is_foreign', 'func', 'func_sub', 'econ', 'econ_sub',
                    'approved', 'revised', 'executed'))
