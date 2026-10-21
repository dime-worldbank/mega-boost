# Databricks notebook source
import dlt
from pyspark.sql.functions import coalesce, col, initcap, lit, lower, regexp_replace, substring, trim, when
from pyspark.sql.types import DoubleType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Burundi'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'bdi_boost_bronze')
def boost_bronze():
    bronze_df = (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/Expenditure.csv')
    )
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name=f'bdi_boost_silver')
def boost_silver():
    # Each condition is the SUMIFS criteria of one row of "Burundi BOOST.xlsx" (row numbers are the
    # Executed sheet's; approved uses the same criteria). The workbook uses ministry codes in 2013-2015
    # and COFOG codes from 2016; judiciary, public safety and agriculture go back to ministry codes after 2016.
    # Where the workbook counts a line in two rows, the order of the when() decides which keeps it
    from_2013_to_2015 = (col('year') >= 2013) & (col('year') <= 2015)
    from_2016_to_2017 = (col('year') >= 2016) & (col('year') <= 2017)
    from_2019_to_2024 = (col('year') >= 2019) & (col('year') <= 2024)
    from_2016_to_2024 = from_2016_to_2017 | from_2019_to_2024 # no 2018 data
    not_2016 = from_2013_to_2015 | (col('year') == 2017) | from_2019_to_2024
    water_admin2 = ['44012', '44013', '44014', '42011', '44503', '44510', '42503']
    housing_admin2 = ['45017', '09018', '45016', '44011', '44008', '45013', '11019']
    # social assistance (row 18), used by func, econ_sub and econ
    social_assistance = col('Econ_3').startswith('672')
    # wage bill (row 4), used by both econ_sub and econ
    wage_bill = col('Econ_1').startswith('1 ') | (col('Econ_4') == '6212 Stage de premier emploi pour 250 jeunes')
    # water and sanitation (row 205), used by both func and func_sub. The workbook lists admin2 42503 twice
    water_and_sanitation = (
        (from_2013_to_2015 & (
            substring(col('Admin_2'), 1, 5).isin(water_admin2) |
            col('Econ_4').startswith('2132 Reseaux adduction'))) |
        (from_2016_to_2024 & col('func2').startswith('7062')))
    return (dlt.read(f'bdi_boost_bronze')
        .withColumn('Econ_1', coalesce(col('Econ_1'), lit('')))
        .withColumn('Econ_3', coalesce(col('Econ_3'), lit('')))
        .withColumn('Econ_4', coalesce(col('Econ_4'), lit('')))
        .withColumn('func1', coalesce(col('func1'), lit('')))
        .withColumn('func2', coalesce(col('func2'), lit('')))
        .withColumn('func3', coalesce(col('func3'), lit('')))
        .withColumn('Admin_1', coalesce(col('Admin_1'), lit('')))
        .withColumn('Admin_2', coalesce(col('Admin_2'), lit('')))
        .withColumn('Geo', coalesce(col('Geo'), lit('')))
        .withColumn('road', coalesce(col('road'), lit('')))
        .withColumn('environment', coalesce(col('environment'), lit('')))
        .withColumn('year', col('Year').cast('int'))
        # total expenditures (row 2)
        .filter(~col('Econ_1').startswith('9 '))
        # 2015 has no execution data: the workbook scales approved by the 2014 execution rate
        # (Executed!K2 / approved!K2). The Executed sheet has no 2024 values
        .withColumn('executed',
            when(col('year') == 2015, col('Credit') * lit(719503643911 / 793650121655))
            .when(col('year') != 2024, col('Ordered_to_pay'))
        ).withColumn('admin0', lit('Central')
        ).withColumn('admin1', lit('Central Scope')
        ).withColumn('admin2',
            initcap(trim(regexp_replace(col('Admin_1'), '^[0-9\\s]*', '')))
        ).withColumn('geo1',
            when((col('Geo') == '') | col('Geo').startswith('00') | lower(col('Geo')).contains('n/a'), 'Central Scope')
            .otherwise(initcap(trim(regexp_replace(col('Geo'), '^[0-9\\s]*', ''))))
        ).withColumn('geo1',
            when(col('geo1').isin('Bujumbura Mairie', 'Bujumbura - Mairie'), 'Mairie de Bujumbura')
            .when(col('geo1') == 'Bujumbura Rural', 'Bujumbura')
            .when(col('geo1') == 'Kirundi', 'Kirundo')
            .otherwise(col('geo1'))
        # Burundi is a special case where geo has more information than admin: all spending is by the
        # central government (admin0 = Central), but Geo tells which province it was spent in
        ).withColumn('geo0',
            when(col('geo1') == 'Central Scope', 'Central')
            .otherwise('Regional')
        ).withColumn('func',
            # social protection (row 257). Expert decision: social assistance (672) only in 2013-2015 and 2019-2024,
            # and no other function keeps those lines in 2013-2015
            when((from_2013_to_2015 & social_assistance) |
                 (from_2016_to_2017 & col('func1').startswith('710')) |
                 (from_2019_to_2024 & social_assistance), 'Social protection')
            # housing (row 203 = water and sanitation + the housing units). Needs to be before economic affairs:
            # the water units of ministry 42 are also in energy
            .when((from_2013_to_2015 & ~social_assistance & (
                      water_and_sanitation |
                      substring(col('Admin_2'), 1, 5).isin(housing_admin2))) |
                  (from_2016_to_2024 & col('func1').startswith('706')), 'Housing and community amenities')
            # defence (row 28)
            .when(col('Admin_1').startswith('13 ') & ~(from_2013_to_2015 & social_assistance), 'Defence')
            # public order and safety (row 30 = judiciary + public safety)
            .when((not_2016 & ~(from_2013_to_2015 & social_assistance) & (
                      substring(col('Admin_1'), 1, 2).isin('74', '75', '76') |
                      substring(col('Admin_1'), 1, 3).isin('16 ', '11 '))) |
                  ((col('year') == 2016) & col('func1').startswith('703')), 'Public order and safety')
            # environmental protection (row 193)
            .when((from_2013_to_2015 & ~social_assistance & (lower(col('environment')) == 'y')) |
                  (from_2016_to_2024 & col('func1').startswith('705')), 'Environmental protection')
            # health (row 215)
            .when((from_2013_to_2015 & ~social_assistance & (
                      col('Admin_1').startswith('33 ') |
                      col('Econ_4').startswith('2133 Reseaux d'))) |
                  (from_2016_to_2024 & col('func1').startswith('707')), 'Health')
            # recreation, culture and religion (row 233)
            .when((from_2013_to_2015 & ~social_assistance & col('Admin_1').startswith('37 ')) |
                  (from_2016_to_2024 & col('func1').startswith('708')), 'Recreation, culture and religion')
            # education (row 235)
            .when((from_2013_to_2015 & ~social_assistance & substring(col('Admin_1'), 1, 3).isin('31 ', '32 ')) |
                  (from_2016_to_2024 & col('func1').startswith('709')), 'Education')
            # economic affairs (row 41 = agriculture + transport + energy + telecoms + ministry 41).
            # Expert decision: admin2 42009, 42011 and 42503 are not energy
            .when((from_2013_to_2015 & ~social_assistance &
                   (substring(col('Admin_1'), 1, 3).isin('40 ', '45 ', '42 ', '18 ') | col('Admin_1').startswith('41')) &
                   ~substring(col('Admin_2'), 1, 5).isin('42009', '42011', '42503')) |
                  ((col('year') == 2016) & (
                      substring(col('func2'), 1, 4).isin('7042', '7043', '7045', '7046') |
                      col('Admin_1').startswith('41'))) |
                  (((col('year') == 2017) | from_2019_to_2024) & (
                      col('Admin_1').startswith('40 ') |
                      substring(col('func2'), 1, 4).isin('7043', '7045', '7046') |
                      col('Admin_1').startswith('41'))), 'Economic affairs')
            # general public services (row 24 = total - the rows above)
            .otherwise('General public services')
        ).withColumn('func_sub',
            # a sub-function is only kept under the function that owns the line
            when(col('func') == 'Public order and safety',
                # judiciary (row 32)
                when((not_2016 & (
                          substring(col('Admin_1'), 1, 2).isin('74', '75', '76') |
                          col('Admin_1').startswith('16 '))) |
                     ((col('year') == 2016) & col('func2').startswith('7033')), 'Judiciary')
                # public safety (row 36)
                .when((not_2016 & col('Admin_1').startswith('11 ')) |
                      ((col('year') == 2016) & col('func1').startswith('703') & ~col('func2').startswith('7033')), 'Public Safety')
            )
            .when(col('func') == 'Housing and community amenities',
                # water and sanitation (row 205)
                when(water_and_sanitation, 'Water Supply')
            )
            .when(col('func') == 'Economic affairs',
                # agriculture (row 43)
                when((not_2016 & col('Admin_1').startswith('40 ')) |
                     ((col('year') == 2016) & col('func2').startswith('7042')), 'Agriculture')
                # roads (row 76)
                .when((from_2013_to_2015 & (lower(col('road')) == 'y') &
                       ~substring(col('Admin_2'), 1, 5).isin('45513', '45523')) |
                      (from_2016_to_2024 & col('func3').startswith('70451')), 'Roads')
                # railroads (row 91)
                .when((from_2013_to_2015 & substring(col('Admin_2'), 1, 5).isin('45513', '45523', '41515', '41523')) |
                      (from_2016_to_2024 & col('func3').startswith('70453')), 'Railroads')
                # water transport (row 104), no formula before 2016
                .when(from_2016_to_2024 & col('func3').startswith('70452'), 'Water Transport')
                # air transport (row 117)
                .when((from_2013_to_2015 & (
                          col('Econ_4').startswith('2136') |
                          col('Econ_4').startswith('2128 Construction cloture') |
                          col('Admin_2').startswith('45528'))) |
                      (from_2016_to_2024 & col('func3').startswith('70454')), 'Air Transport')
                # transport (row 63), what is left of it after the four modes above
                .when((from_2013_to_2015 & col('Admin_1').startswith('45 ')) |
                      (from_2016_to_2024 & col('func2').startswith('7045')), 'Transport')
                # energy (row 130). Expert decision: admin2 42009, 42011 and 42503 are not energy
                .when((from_2013_to_2015 & col('Admin_1').startswith('42 ') &
                       ~substring(col('Admin_2'), 1, 5).isin('42009', '42011', '42503')) |
                      (from_2016_to_2024 & col('func2').startswith('7043')), 'Energy')
                # telecoms (row 188)
                .when((from_2013_to_2015 & col('Admin_1').startswith('18 ')) |
                      (from_2016_to_2024 & col('func2').startswith('7046')), 'Telecom')
            )
            .when(col('func') == 'Education',
                # primary education (row 241), no formula before 2016
                when(from_2016_to_2024 & col('func2').startswith('7091'), 'Primary Education')
                # secondary education (row 243), no formula before 2016
                .when(from_2016_to_2024 & col('func2').startswith('7092'), 'Secondary Education')
                # tertiary education (row 247)
                .when((from_2013_to_2015 & col('Admin_1').startswith('31 ')) |
                      (from_2016_to_2024 & col('func2').startswith('7094')), 'Tertiary Education')
                # primary and secondary education (row 245 = education - tertiary in 2013-2015)
                .when(from_2013_to_2015 & col('Admin_1').startswith('32 '), 'Primary and Secondary education')
            )
        ).withColumn('econ_sub',
            # social assistance (row 18)
            when(social_assistance, 'Social Assistance')
            # other social benefits (row 20)
            .when(col('Econ_3').startswith('673'), 'Other Social Benefits')
            # allowances (row 6)
            .when(col('Econ_3').startswith('614') | col('Econ_3').startswith('615'), 'Allowances')
            # basic wages (row 5 = wage bill - allowances)
            .when(wage_bill, 'Basic Wages')
            # basic services (row 12)
            .when(col('Econ_3').startswith('624') | col('Econ_3').startswith('635'), 'Basic Services')
            # employment contracts (row 13)
            .when(col('Econ_3').startswith('627'), 'Employment Contracts')
            # recurrent maintenance (row 14)
            .when(col('Econ_3').startswith('625'), 'Recurrent Maintenance')
            # subsidies to production (row 16)
            .when(col('Econ_1').startswith('5 '), 'Subsidies to Production')
        ).withColumn('econ',
            # wage bill (row 4)
            when(wage_bill, 'Wage bill')
            # social benefits (row 17)
            .when(col('econ_sub').isin('Social Assistance', 'Other Social Benefits'), 'Social benefits')
            # capital expenditures (row 8)
            .when(col('Econ_1').startswith('4 '), 'Capital expenditures')
            # goods and services (row 11)
            .when(col('Econ_1').startswith('2 '), 'Goods and services')
            # subsidies (row 15)
            .when(col('Econ_1').startswith('5 '), 'Subsidies')
            # other grants and transfers (row 21)
            .when(col('Econ_3').startswith('664'), 'Other grants and transfers')
            # interest on debt (row 26)
            .when(col('Econ_1').startswith('3 '), 'Interest on debt')
            # other expenses (row 22 = total - the rows above)
            .otherwise('Other expenses')
        )
    )

@dlt.table(name=f'bdi_boost_gold')
def boost_gold():
    return (dlt.read(f'bdi_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                col('Credit').alias('approved').cast(DoubleType()),
                lit(None).cast(DoubleType()).alias('revised'),
                col('executed').cast(DoubleType()),
                'admin0',
                'admin1',
                'admin2',
                'geo0',
                'geo1',
                # the workbook has no foreign funding formula
                lit(None).cast('boolean').alias('is_foreign'),
                'func',
                'func_sub',
                'econ',
                'econ_sub'
        )
    )
