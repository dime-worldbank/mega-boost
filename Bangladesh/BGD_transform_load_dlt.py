# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import col, lower, initcap, trim, regexp_extract, regexp_replace, when, lit, substring, expr
from pyspark.sql.types import StringType


TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Bangladesh'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'bgd_2019_onward_boost_bronze')
def boost_2019_onward_bronze():
    # TODO: expand to 2018 and before, which have diff columns
    file_paths = [f"{COUNTRY_MICRODATA_DIR}/2019.csv", f"{COUNTRY_MICRODATA_DIR}/202*.csv"]

    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(file_paths))
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df

@dlt.table(name=f'bgd_2019_onward_boost_silver')
def boost_2019_onward_silver():
    return (dlt.read(f'bgd_2019_onward_boost_bronze')
        .filter(col('ECON1').isin(['3 - Recurrent Expenditure', '4 - Capital Expenditure']))
        .withColumn(
            'geo1',
            when(
                col("GEO1") == 'Dhaka', col("GEO1")
            ).when(
                # necessary for coercing other values to None
                regexp_extract(col("GEO1"), r'\d{2}', 0) != '',
                regexp_replace(col("GEO1"), r"\d+ ", "") # remove leading numbers
            ).otherwise('Central Scope')
        ).withColumn(
            'admin0', 
            when((col('ADMIN0').endswith('CAFO') | col('ADMIN0').endswith('DAFO')), 'Regional')
            .otherwise('Central')
        ).withColumn(
            'admin1',
            when(col('admin0') == 'Central', 'Central Scope')
            .otherwise(col('geo1'))
        ).withColumn(
            'admin2',
            col('ADMIN2')
        ).withColumn(
            'func',
            when(col('FUNC2') == 'Defence Services', 'Defence')
            .when(col('FUNC2') == 'Public Order and Safety', 'Public order and safety')
            .when(col('FUNC2') == 'Housing', 'Housing and community amenities')
            .when(col('FUNC2') == 'Health', 'Health')
            .when(col('FUNC2') == 'Recreation, Culture and Religious Affairs', 'Recreation, culture and religion')
            .when(col('FUNC2') == 'Education and Technology', 'Education')
            .when(col('FUNC2') == 'Social Security and Welfare', 'Social protection')
            .when(
                col('ADMIN2').isin([
                    "145-Ministry of Environment, Forest and Climate Change",
                    "14503-Department of Environment"
                ]),
                'Environmental protection')
            .when(
                ((col('FUNC1') == 'Economic Sector')
                & (~col('FUNC2').isin(['Housing', 'Local Government and Rural Development']))),
                'Economic affairs')
            .otherwise(lit('General public services'))
        )
        .withColumn(
            'func_sub',
            when(col('func') == 'Public order and safety', 
                 when(col('ADMIN2') == '122-Public Security Division', 'Public Order')
                 .otherwise('Judiciary')
            )
            .when(col('func') == 'Education', 
                when(
                    (col('ADMIN2').contains('Primary Education') 
                    | col('ADMIN2').contains('Primary and Mass Education'))
                , 'Primary Education')
                .when(
                    (col('ADMIN2').contains('Secondary and Higher Education') 
                    | col('ADMIN2').contains('Secondary & Higher Education'))
                , 'Secondary Education')
            )
        ).withColumn(
            'econ_sub',
            when(col('ECON3') == "311 - Wages and Salaries", 
                when(col('ECON_5') == '31113 - Allowances', 'Allowances')
                .otherwise('Basic Wages')
            )
            .when(
                (col('Old_ECON3').startswith("3211113")
                 | col('Old_ECON3').startswith("3211114")
                 | col('Old_ECON3').startswith("3211115")
                 | col('Old_ECON3').startswith("3211120")
                 | col('Old_ECON3').startswith("3211122")
                 | col('Old_ECON3').startswith("3211129")
                ), 'Basic Services')
            .when(
                col('ECON_4') == '3257 - Professional services, honorariums and special expenses',
                'Employment Contracts')
            .when((col('ECON3').startswith('372')), 'Social Assistance')
            .when(col('ECON_5') == '37311 - Employment-related social benefits in cash', 'Pensions')
        ).withColumn(
            'econ',
            when(col('ECON1') == '4 - Capital Expenditure', 'Capital expenditures')
            .when(col('ECON2') == '31 - Compensation of Employees', 'Wage bill')
            .when(col('ECON2') == '32 - Purchases of Goods and Services', 'Goods and services')
            .when(col('ECON2') == '34 - Interest', 'Interest on debt') 
            .when(col('ECON2') == '35 - Subsidies', 'Subsidies')
            .when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits')
            .otherwise('Other expenses') 
        )
    )

# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'bgd_2015_to_2018_boost_bronze')
def boost_2015_to_2018_bronze():
    file_paths = list(f"{COUNTRY_MICRODATA_DIR}/{year}.csv" for year in range(2015, 2019))

    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(file_paths))
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df

@dlt.table(name=f'bgd_2015_to_2018_boost_silver')
def boost_2015_to_2018_silver():
    return (dlt.read(f'bgd_2015_to_2018_boost_bronze')
        .filter(~col('ECON2').isin([
            "6700-Revenue - General",
            "7100-Investments in Shares and Equities",
            "7300-Loans",
            "7400-Advances to Government Employees",
            "7500-Term Debt Repayments",
            "7600-Floating Debt Repayments",
            "7700-Foreign Debt Repayment",
            "7800-Transaction with IMF",
            "7900-Development Import Duty and VAT",
        ]))
        .withColumn(
            'admin0', 
            when(col('ADMIN0').isin(['DAO', 'UAO']), 'Regional')
            .otherwise('Central')
        ).withColumn(
            'geo1', 
            when(
                # necessary for coercing other values to None
                regexp_extract(col("GEO1"), r'\d{2}', 0) != '',
                regexp_replace(col("GEO1"), r"\d+ ", "") # remove leading numbers
            ).otherwise('Central Scope')
        ).withColumn(
            'admin1',
            when(col('admin0') == 'Central', 'Central Scope')
            .otherwise(col('geo1'))
        ).withColumn(
            'admin2',
            col('ADMIN2')
        ).withColumn(
            'func',
            # social protection needs to be first to pick up all of ECON2 6300-Pensions and Gratuities
            when(
                ((col('FUNC1') == 'Social Security and Welfare') | col('ECON2').startswith('6300')),
                'Social protection'
            ).when(
                col('FUNC1') == 'Defence Services',
                'Defence'
            ).when(
                col('FUNC1') == 'Public Order and Safety',
                'Public order and safety'
            ).when(
                col('FUNC1') == 'Housing',
                'Housing and community amenities'
            ).when(
                col('FUNC1') == 'Health',
                'Health'
            ).when(
                col('FUNC1') == 'Recreation, Culture and Religious Affairs',
                'Recreation, culture and religion'
            ).when(
                col('FUNC1') == 'Education and Technology',
                'Education'
            ).when(
                # Environment needs to be before Economic affairs as it's a subcategory of FUNC1='Agriculture'
                col('ADMIN2') == "45-Ministry of Environment and Forest",
                'Environmental protection'
            ).when(
                col('FUNC1').isin([
                    "Agriculture",
                    "Energy and Power",
                    "Transport and Communication",
                    "Industrial and Economic Services"
                ]),
                'Economic affairs'
            ).otherwise(lit('General public services'))
        )
        .withColumn(
            'func_sub',
            when(col('func') == 'Public order and safety', 
                 when(col('ADMIN2') == '22-Public Security Division', 'Public Order')
                 .otherwise('Judiciary')
            )
            .when(col('func') == 'Education', 
                when(
                    (col('ADMIN2').contains('Primary Education') 
                    | col('ADMIN2').contains('Primary and Mass Education'))
                , 'Primary Education')
                .when(
                    (col('ADMIN2').contains('Secondary and Higher Education') 
                    | col('ADMIN2').contains('Secondary & Higher Education'))
                , 'Secondary Education')
            )
        ).withColumn(
            'econ_sub',
            when(col('ECON2').isin(["4500-Pay of Officers","4600-Pay of Establishment"]), 'Basic Wages')
            .when(col('ECON2') == "4700-Allowances", 'Allowances')
            .when(
                col('ECON2') == "4800-Supplies and Services",
                when(
                    (col('ECON3').startswith("4806")
                     | col('ECON3').startswith("4807")
                     | col('ECON3').startswith("4808")
                     | col('ECON3').startswith("4816")
                     | col('ECON3').startswith("4819")
                     | col('ECON3').startswith("4821")
                     | col('ECON3').startswith("4822")
                    ), 'Basic Services'
                ).when(
                    (col('ECON3').startswith("4849")
                     | col('ECON3').startswith("4851")
                     | col('ECON3').startswith("4874")
                    ), 'Employment Contracts'
                )
            )
            .when(
                col('ECON2') == '6300-Pensions and Gratuities',
                'Pensions')
            .when(
                (col('ECON2').startswith('59') & (col('FUNC1') == 'Social Security and Welfare')),
                'Social Assistance'
            )
        ).withColumn(
            'econ',
            when(
                col('ECON1') == "6800-Capital Expenditure",
                'Capital expenditures')
            .when(
                col('econ_sub').isin([
                    'Basic Wages',
                    'Allowances']),
                'Wage bill')
            .when(
                col('ECON2').isin([
                    "4800-Supplies and Services",
                    "4900-Repairs and Maintenance"]),
                'Goods and services')
            .when(
                col('ECON2').isin([
                    "5000-Term Loan Interest",
                    "5100-Floating Loan Interest",
                    "5200-Interest on National Savings Certificates",
                    "5300-Provident Fund Interest",
                    "5400-Postal Life Insurance Interest",
                    "5500-Other Interest",
                    "5600-Interest on Foreign Debt"
                ]),
                'Interest on debt'
            ) 
            .when(
                col('ECON2').isin([
                    "6400-State Trading",
                    "5800-Subsidies and Incentives"]),
                'Subsidies')
            .when(
                col('econ_sub').isin([
                    'Social Assistance',
                    'Pensions']),
                'Social benefits')
            .otherwise('Other expenses') 
        )
    )


# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name=f'bgd_2008_to_2014_boost_bronze')
def boost_2008_to_2014_bronze():
    file_paths = list(f"{COUNTRY_MICRODATA_DIR}/{year}.csv" for year in range(2008, 2015))

    bronze_df = (spark.read
                 .format("csv")
                 .options(**CSV_READ_OPTIONS)
                 .option("inferSchema", "true")
                 .load(file_paths))
    bronze_df = bronze_df.withColumn('year', col('year').cast('int'))
    bronze_df = bronze_df.dropna(how='all')
    return bronze_df

@dlt.table(name=f'bgd_2008_to_2014_boost_silver')
def boost_2008_to_2014_silver():
    return (dlt.read(f'bgd_2008_to_2014_boost_bronze')
        .filter((regexp_extract(col("ECON1"), r'\d', 0) != '') &
            ~(col('ECON1').startswith("71")
             | col('ECON1').startswith("72")
             | col('ECON1').startswith("73")
             | col('ECON1').startswith("74")
             | col('ECON1').startswith("75")
             | col('ECON1').startswith("76")
             | col('ECON1').startswith("77")
             | col('ECON1').startswith("79")
            )
        ).withColumn(
            'admin0', 
            when(col('ADMIN0').isin(['DAO', 'UAO']), 'Regional')
            .otherwise('Central')
        ).withColumn(
            'geo1', 
            when(
                col('GEO1') == 'khulna', 'Khulna'
            ).when(
                # necessary for coercing other values to None
                regexp_extract(col("GEO1"), r'\d{2}', 0) != '',
                regexp_replace(col("GEO1"), r"\d+ ", "") # remove leading numbers
            ).otherwise('Central Scope')
        ).withColumn(
            'admin1',
            when(col('admin0') == 'Central', 'Central Scope')
            .otherwise(col('geo1'))
        ).withColumn(
            'admin2',
            col('ADMIN2')
        ).withColumn(
            'func',
            # social protection needs to be first to pick up all of ECON1 63 Pensions and Gratuities
            when(
                ((col('FUNC1') == 'Social Security and Welfare') | (col('ECON1') == '63 Pensions and Gratuities')),
                'Social protection'
            ).when(
                col('FUNC1') == 'Defence Services',
                'Defence'
            ).when(
                col('FUNC1') == 'Public Order and Safety',
                'Public order and safety'
            ).when(
                col('FUNC1') == 'Housing',
                'Housing and community amenities'
            ).when(
                col('FUNC1') == 'Health',
                'Health'
            ).when(
                col('FUNC1') == 'Recreation, Culture and Religious Affairs',
                'Recreation, culture and religion'
            ).when(
                col('FUNC1') == 'Education & Technology',
                'Education'
            ).when(
                # Environment needs to be before Economic affairs as it's a subcategory of FUNC1='Agriculture'
                col('ADMIN2') == "45 Ministry of Environment and Forest",
                'Environmental protection'
            ).when(
                col('FUNC1').isin([
                    "Agriculture",
                    "Energy and Power",
                    "Transport and Communication",
                    "Industrial and Economic Services"
                ]),
                'Economic affairs'
            ).otherwise(lit('General public services'))
        )
        .withColumn(
            'func_sub',
            when(col('func') == 'Public order and safety', 
                 when(col('ADMIN2') == '22 Ministry of Home Affairs', 'Public Order')
                 .otherwise('Judiciary')
            )
            .when(col('func') == 'Education', 
                when(
                    (col('ADMIN2').contains('Primary Education') 
                    | col('ADMIN2').contains('Primary and Mass Education'))
                , 'Primary Education')
                .when(
                    col('ADMIN2') == '25 Ministry of Education'
                , 'Secondary Education')
            )
        ).withColumn(
            'econ_sub',
            when(col('ECON1').isin(["45 Pay of Officers","46 Pay of Establishment"]), 'Basic Wages')
            .when(col('ECON1') == "47 Allowances", 'Allowances')
            .when(
                col('ECON1') == "48 Supplies and Services",
                when(
                    col('ECON2').isin([
                        "4823 Petrol, Oil and Lubricants",
                        "4821 Electricity",
                        "4891 Subsistence",
                        "4872 Diet",
                        "4806 Rent - Office",
                        "4819 Water"
                    ]),
                    'Basic Services'
                ).when(
                    col('ECON2') == "4883 Honorarium/Fees/Remuneration",
                    'Employment Contracts'
                )
            )
            .when(
                col('ECON1') == '63 Pensions and Gratuities',
                'Pensions')
            .when(
                ((col('ECON1') == '59 Grants-in-Aid') & (col('FUNC1') == 'Social Security and Welfare')),
                'Social Assistance'
            )
        ).withColumn(
            'econ',
            when(
                (col('ECON1').startswith("68")
                 | col('ECON1').startswith("69")
                 | col('ECON1').startswith("70")
                ),
                'Capital expenditures')
            .when(
                col('econ_sub').isin([
                    'Basic Wages',
                    'Allowances']),
                'Wage bill')
            .when(
                col('ECON1').isin([
                    "48 Supplies and Services",
                    "49 Repairs, Maintenance and Rehabilitation"]),
                'Goods and services')
            .when(
                col('ECON1').isin([
                    "50 Term Loan Interest Repayment",
                    "51 Floating Loan Interest",
                    "52 Interest on National Savings Certificates",
                    "53 Provident Fund Interest",
                    "55 Other Interest",
                    "56 Interest on Foreign Debt",
                ]),
                'Interest on debt'
            ) 
            .when(
                col('ECON1') == "58 Subsidies",
                'Subsidies')
            .when(
                col('econ_sub').isin([
                    'Social Assistance',
                    'Pensions']),
                'Social benefits')
            .otherwise('Other expenses') 
        )
    )

# COMMAND ----------

@dlt.table(name=f'bgd_boost_gold')
def boost_gold():
    gold_cols = [
        'year',
        col('BUDGET').alias('approved'),
        col('REVISED').alias('revised'),
        col('EXECUTED').alias('executed'),
        'admin0',
        'admin1',
        'admin2',
        'geo1',
        'func',
        'func_sub',
        'econ',
        'econ_sub',
    ]

    return (
        dlt.read(f'bgd_2019_onward_boost_silver')
        .select(*gold_cols)
        .union(
            dlt.read(f'bgd_2015_to_2018_boost_silver')
            .select(*gold_cols)
        )
        .union(
            # 2008 data FUNC1 is not propertly tagged, exclude entirely for correness sake
            dlt.read(f'bgd_2008_to_2014_boost_silver')
            .filter(col('year') != 2008)
            .select(*gold_cols)
        )
        .withColumn('country_name', lit(COUNTRY))
    )

