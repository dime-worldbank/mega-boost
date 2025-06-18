# Databricks notebook source
TARGET_TABLE = 'prd_mega.boost.boost_gold'

coverage = spark.sql(f"""
    SELECT MIN(year) AS coverage_start, MAX(year) AS coverage_end 
    FROM {TARGET_TABLE}
""").collect()

coverage_start = coverage[0]['coverage_start']
coverage_end = coverage[0]['coverage_end']
print(f'{TARGET_TABLE} coverage from {coverage_start} to {coverage_end}')

# COMMAND ----------

countries = spark.sql(f"""
    SELECT DISTINCT country_name 
    FROM {TARGET_TABLE}
    WHERE country_name IS NOT NULL
    ORDER BY country_name
""").collect()

coverage_countries = ', '.join(
    sorted([ # Handle comma in country name, e.g. Congo, Dem. Rep.
        f'"{row["country_name"]}"' if ',' in row["country_name"] else row["country_name"]
        for row in countries
    ])
)
print(f'Coverage countries: {coverage_countries}')

# COMMAND ----------

tagging_sql = f"""
ALTER TABLE {TARGET_TABLE} SET TAGS (
    'name' = 'BOOST Harmonized',
    'subject' = 'Finance',
    'classification' = 'Official Use Only',
    'category' = 'Public Sector',
    'subcategory' = 'Financial Management',
    'frequency' = 'Annually',
    'collections' = 'Financial Management (FM), BOOST - Public Expenditure Database',
    'source' = 'BOOST',
    'domain' = 'Budget',
    'subdomain' = 'Budget & Cost Accounting',
    'destinations' = 'dataexplorer, ddh',
    'exception' = '7. Member Countries/Third Party Confidence',
    'license' = 'License Not Applicable (AMS 6.21A)',
    'topics' = 'Economic Growth, Macroeconomic and Structural Policies, Public Sector Management',
    'coverage_year_start' = '{coverage_start}',
    'coverage_year_end' = '{coverage_end}',
    'coverage_countries' = '{coverage_countries}',
    'team_lead' = 'mmastruzzi@worldbank.org',
    'collaborators' = 'icapita@worldbank.org, agirongordillo@worldbank.org, sbhupatiraju@worldbank.org, ysuzuki2@worldbank.org, elysenko@worldbank.org, wlu4@worldbank.org'
);
"""

spark.sql(tagging_sql)
