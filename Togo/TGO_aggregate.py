# Databricks notebook source
# Builds the tables the RPF country dashboard reads, for Togo only, in pandas
# without Spark. No quality checks.
#
# Inputs
#   GOLD_CSV       tgo_boost_gold.csv written by TGO_ETL.py
#   INDICATOR_DIR  one CSV per prd_mega.indicator table, named after the table:
#                  consumer_price_index, population, subnational_population,
#                  poverty_rate, global_data_lab_hd_index, edu_spending,
#                  health_expenditure (Databricks exports write nulls as "null")
#   OUTPUT_DIR     where the output CSVs go
# TODO: Refactor the cross-country aggregation so the logic lives in one place and every country can reuse it.
import os

import numpy as np
import pandas as pd

COUNTRY = 'Togo'
GOLD_CSV = os.environ.get('GOLD_CSV') or input("Path to tgo_boost_gold.csv: ").strip()
INDICATOR_DIR = os.environ.get('INDICATOR_DIR') or input("Folder with the indicator CSVs: ").strip()
OUTPUT_DIR = os.environ.get('OUTPUT_DIR') or input("Output folder: ").strip()


def read_indicator(name):
    df = pd.read_csv(os.path.join(INDICATOR_DIR, f'{name}.csv'), na_values=['null'])
    return df[df['country_name'] == COUNTRY].reset_index(drop=True)


cpi = read_indicator('consumer_price_index')
population = read_indicator('population')[['country_name', 'year', 'population']]
subnational_population = read_indicator('subnational_population')
poverty_rate = read_indicator('poverty_rate')
hd_index = read_indicator('global_data_lab_hd_index')
edu_spending = read_indicator('edu_spending')
health_expenditure = read_indicator('health_expenditure')

# COMMAND ----------

# boost_gold: geo0 follows geo1, and rows with neither an executed nor an
# approved amount are dropped
gold = pd.read_csv(GOLD_CSV, na_values=['null'])
gold['adm1_name'] = gold['geo1']
gold['geo0'] = np.where((gold['geo1'] == 'Central Scope') | gold['geo1'].isna(), 'Central', 'Regional')
gold = gold[['country_name', 'year', 'admin0', 'admin1', 'admin2', 'geo0', 'geo1', 'adm1_name',
             'func', 'func_sub', 'econ', 'econ_sub', 'is_foreign', 'approved', 'revised', 'executed']]
boost_gold = gold[(gold['executed'].fillna(0) != 0) | (gold['approved'].fillna(0) != 0)].reset_index(drop=True)
boost_gold['is_foreign'] = boost_gold['is_foreign'].fillna(False).astype(bool)

# cpi_factor: CPI relative to the earliest BOOST year, which must be in the CPI file
base_year = boost_gold['year'].min()
base_cpi = cpi.loc[cpi['year'] == base_year, 'cpi']
assert len(base_cpi) == 1, f'expected one CPI row for {COUNTRY} {base_year}, found {len(base_cpi)}'
cpi_factor = cpi[['country_name', 'year']].assign(cpi_factor=cpi['cpi'] / base_cpi.iloc[0])

# COMMAND ----------

# Aggregates. Years without a CPI row drop out at the cpi_factor join, as in
# Databricks. sum(min_count=1) keeps Spark's rule that a sum over no values is
# null rather than 0, and dropna=False keeps null func_sub/econ_sub as groups.

# expenditure_by_country_year
regional = boost_gold['admin0'] == 'Regional'
t = (boost_gold.assign(
        decentralized_expenditure=boost_gold['executed'].where(regional),
        foreign_funded_expenditure=boost_gold['executed'].where(boost_gold['is_foreign']),
        decentralized_budget=boost_gold['approved'].where(regional),
        foreign_funded_budget=boost_gold['approved'].where(boost_gold['is_foreign']))
    .groupby(['country_name', 'year'])
    [['executed', 'decentralized_expenditure', 'foreign_funded_expenditure',
      'approved', 'decentralized_budget', 'foreign_funded_budget']].sum(min_count=1)
    .rename(columns={'executed': 'expenditure', 'approved': 'budget'}).reset_index()
    .merge(cpi_factor, on=['country_name', 'year'])
    .merge(population, on=['country_name', 'year'], how='left'))
t['expenditure_decentralization'] = t['decentralized_expenditure'] / t['expenditure']
t['expenditure_foreign_ratio'] = t['foreign_funded_expenditure'] / t['expenditure']
t['budget_decentralization'] = t['decentralized_budget'] / t['budget']
t['budget_foreign_ratio'] = t['foreign_funded_budget'] / t['budget']
t['real_expenditure'] = t['expenditure'] / t['cpi_factor']
t['real_budget'] = t['budget'] / t['cpi_factor']
t['latest_year'] = t['year'].max()
t['earliest_year'] = t['year'].min()
t['per_capita_expenditure'] = t['expenditure'] / t['population']
t['per_capita_real_expenditure'] = t['real_expenditure'] / t['population']
t['per_capita_budget'] = t['budget'] / t['population']
t['per_capita_real_budget'] = t['real_budget'] / t['population']
expenditure_by_country_year = t

# expenditure_by_country_geo1_func_year: regions join their own population,
# Central Scope the sum of all regions
pop = subnational_population[['country_name', 'year', 'population', 'adm1_name']]
pop_central = (pop.groupby(['country_name', 'year'])['population'].sum(min_count=1)
    .reset_index().assign(adm1_name='Central Scope'))
pop = pd.concat([pop_central, pop], ignore_index=True)
t = (boost_gold.groupby(['country_name', 'adm1_name', 'func', 'year'], dropna=False)
    [['executed', 'approved']].sum(min_count=1)
    .rename(columns={'executed': 'expenditure', 'approved': 'budget'}).reset_index()
    .merge(cpi_factor, on=['country_name', 'year'])
    .merge(pop, on=['country_name', 'adm1_name', 'year']))
t['real_expenditure'] = t['expenditure'] / t['cpi_factor']
t['real_budget'] = t['budget'] / t['cpi_factor']
t['latest_year'] = t['year'].max()
t['earliest_year'] = t['year'].min()
t['per_capita_expenditure'] = t['expenditure'] / t['population']
t['per_capita_real_expenditure'] = t['real_expenditure'] / t['population']
t['per_capita_budget'] = t['budget'] / t['population']
t['per_capita_real_budget'] = t['real_budget'] / t['population']
t['adm1_name_for_map'] = t['adm1_name'] + ', ' + COUNTRY
t['spent_in_region'] = np.where(t['adm1_name'] == 'Central Scope', 'Unspecified', 'Subnational')
expenditure_by_country_geo1_func_year = t

# expenditure_by_country_geo1_year
g = expenditure_by_country_geo1_func_year.groupby(['country_name', 'adm1_name', 'adm1_name_for_map', 'year'])
t = g[['expenditure', 'real_expenditure', 'per_capita_expenditure', 'per_capita_real_expenditure',
       'budget', 'real_budget', 'per_capita_budget', 'per_capita_real_budget']].sum(min_count=1)
t['earliest_year'] = g['earliest_year'].min()
t['latest_year'] = g['latest_year'].max()
expenditure_by_country_geo1_year = t.reset_index()

# expenditure_by_country_admin_func_sub_econ_sub_year
t = (boost_gold.groupby(['country_name', 'year', 'admin0', 'admin1', 'admin2',
                         'func', 'func_sub', 'econ', 'econ_sub', 'is_foreign'], dropna=False)
    [['executed', 'approved']].sum(min_count=1)
    .rename(columns={'executed': 'expenditure', 'approved': 'budget'}).reset_index()
    .merge(cpi_factor, on=['country_name', 'year']))
t['real_expenditure'] = t['expenditure'] / t['cpi_factor']
t['real_budget'] = t['budget'] / t['cpi_factor']
t['earliest_year'] = t.groupby('func', dropna=False)['year'].transform('min')
t['latest_year'] = t.groupby('func', dropna=False)['year'].transform('max')
expenditure_by_country_admin_func_sub_econ_sub_year = t

# expenditure_by_country_geo0_func_sub_year
t = (boost_gold.groupby(['country_name', 'year', 'geo0', 'func', 'func_sub'], dropna=False)
    [['executed', 'approved']].sum(min_count=1)
    .rename(columns={'executed': 'expenditure', 'approved': 'budget'}).reset_index()
    .merge(cpi_factor, on=['country_name', 'year']))
t['real_expenditure'] = t['expenditure'] / t['cpi_factor']
t['real_budget'] = t['budget'] / t['cpi_factor']
t = t[t['real_expenditure'].notna()].reset_index(drop=True)
t['earliest_year'] = t.groupby('func', dropna=False)['year'].transform('min')
t['latest_year'] = t.groupby('func', dropna=False)['year'].transform('max')
expenditure_by_country_geo0_func_sub_year = t

# expenditure_by_country_func_econ_year
a = expenditure_by_country_admin_func_sub_econ_sub_year
regional = a['admin0'] == 'Regional'
central = a['admin0'] == 'Central'
t = (a.assign(
        decentralized_expenditure=a['expenditure'].where(regional),
        central_expenditure=a['expenditure'].where(central),
        domestic_funded_budget=a['budget'].where(~a['is_foreign']),
        decentralized_budget=a['budget'].where(regional),
        central_budget=a['budget'].where(central))
    .groupby(['country_name', 'year', 'func', 'econ'], dropna=False)
    [['expenditure', 'real_expenditure', 'decentralized_expenditure', 'central_expenditure',
      'domestic_funded_budget', 'budget', 'real_budget', 'decentralized_budget', 'central_budget']]
    .sum(min_count=1).reset_index()
    .merge(population, on=['country_name', 'year']))
t['per_capita_expenditure'] = t['expenditure'] / t['population']
t['per_capita_real_expenditure'] = t['real_expenditure'] / t['population']
t['per_capita_budget'] = t['budget'] / t['population']
t['per_capita_real_budget'] = t['real_budget'] / t['population']
expenditure_by_country_func_econ_year = t

# expenditure_by_country_func_year and expenditure_by_country_econ_year
func_econ_sums = ['expenditure', 'real_expenditure', 'decentralized_expenditure', 'central_expenditure',
                  'per_capita_expenditure', 'per_capita_real_expenditure',
                  'budget', 'real_budget', 'decentralized_budget', 'central_budget',
                  'per_capita_budget', 'per_capita_real_budget']

g = expenditure_by_country_func_econ_year.groupby(['country_name', 'year', 'func'], dropna=False)
t = g[func_econ_sums].sum(min_count=1)
t['population'] = g['population'].min()
t = t.reset_index()
t['earliest_year'] = t['year']  # min and max of the year within a year group
t['latest_year'] = t['year']
t['expenditure_decentralization'] = t['decentralized_expenditure'] / t['expenditure']
t['budget_decentralization'] = t['decentralized_budget'] / t['budget']
expenditure_by_country_func_year = t

g = expenditure_by_country_func_econ_year.groupby(['country_name', 'year', 'econ'], dropna=False)
t = g[func_econ_sums].sum(min_count=1)
t['population'] = g['population'].min()
t = t.reset_index()
t['earliest_year'] = t['year']
t['latest_year'] = t['year']
t['budget_decentralization'] = t['decentralized_budget'] / t['budget']
t['expenditure_decentralization'] = t['decentralized_expenditure'] / t['expenditure']
expenditure_by_country_econ_year = t

# COMMAND ----------

# Dashboard pre-query tables

# pov_expenditure_by_country_year: only poverty_rate (income level specific) is needed
pov_expenditure_by_country_year = expenditure_by_country_year.merge(
    poverty_rate.drop(columns=['country_code', 'region', 'poor300', 'poor420', 'poor830', 'data_source'], errors='ignore'),
    on=['country_name', 'year'], how='left')

# expenditure_and_outcome_by_country_geo1_func_year: rank regions by per-capita
# real spending and by outcome (school attendance for Education, health index for Health)
t = expenditure_by_country_geo1_func_year.merge(hd_index, on=['country_name', 'adm1_name', 'year'])
t['outcome_index'] = np.select([t['func'] == 'Education', t['func'] == 'Health'],
                               [t['attendance_6to17yo'], t['health_index']], np.nan)
t = t[t['outcome_index'].notna()].copy()
g = t.groupby(['country_name', 'year', 'func'])
t['rank_per_capita_real_exp'] = g['per_capita_real_expenditure'].rank(method='min', ascending=False).astype('Int64')
t['rank_outcome_index'] = g['outcome_index'].rank(method='min', ascending=False).astype('Int64')
expenditure_and_outcome_by_country_geo1_func_year = expenditure_by_country_geo1_func_year.merge(
    t[['country_name', 'adm1_name', 'year', 'func', 'outcome_index', 'rank_per_capita_real_exp', 'rank_outcome_index']],
    on=['country_name', 'adm1_name', 'year', 'func'], how='left')

# edu_private_expenditure_by_country_year: ICP education spending minus public education spending
edu_public = (expenditure_by_country_func_year[expenditure_by_country_func_year['func'] == 'Education']
    [['country_name', 'year', 'expenditure', 'real_expenditure']]
    .rename(columns={'expenditure': 'pub_expenditure', 'real_expenditure': 'real_pub_expenditure'}))
t = edu_spending.merge(cpi_factor, on=['country_name', 'year']).merge(edu_public, on=['country_name', 'year'])
t['real_edu_spending_current_lcu_icp'] = t['edu_spending_current_lcu_icp'] / t['cpi_factor']
t['expenditure'] = t['edu_spending_current_lcu_icp'] - t['pub_expenditure']
t['real_expenditure'] = t['real_edu_spending_current_lcu_icp'] - t['real_pub_expenditure']
edu_private_expenditure_by_country_year = t[t['expenditure'] >= 0].reset_index(drop=True)

# health_private_expenditure_by_country_year: out-of-pocket share of current health expenditure
t = health_expenditure.merge(cpi_factor, on=['country_name', 'year'])
t['oop_expenditure_current_lcu'] = t['che'] * t['oop_percent_che'] / 100
t['real_expenditure'] = t['oop_expenditure_current_lcu'] / t['cpi_factor']
health_private_expenditure_by_country_year = t

# data_availability: the columns the dashboard reads; year range from boost_gold,
# and Togo is not among the publicly released BOOST datasets
data_availability = pd.DataFrame([{
    'country_name': COUNTRY,
    'boost_earliest_year': boost_gold['year'].min(),
    'boost_latest_year': boost_gold['year'].max(),
    'boost_public': 'No',
    'boost_source_url': 'https://datacatalog.worldbank.org/int/search/dataset/0040663/Togo-BOOST-Public-Expenditure-Database',
}])

# COMMAND ----------

# One CSV per table the dashboard (rpf-country-dash/queries.py) reads
os.makedirs(OUTPUT_DIR, exist_ok=True)
for name, table in [
    ('pov_expenditure_by_country_year', pov_expenditure_by_country_year),
    ('expenditure_by_country_func_econ_year', expenditure_by_country_func_econ_year),
    ('expenditure_by_country_geo0_func_sub_year', expenditure_by_country_geo0_func_sub_year),
    ('expenditure_by_country_geo1_year', expenditure_by_country_geo1_year),
    ('expenditure_and_outcome_by_country_geo1_func_year', expenditure_and_outcome_by_country_geo1_func_year),
    ('edu_private_expenditure_by_country_year', edu_private_expenditure_by_country_year),
    ('health_private_expenditure_by_country_year', health_private_expenditure_by_country_year),
    ('data_availability', data_availability),
]:
    table.to_csv(os.path.join(OUTPUT_DIR, f'{name}.csv'), index=False)
    print(f'{len(table):>7} rows  {name}')
