# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace, initcap, coalesce

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/Congo'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "Exercice IS NOT NULL")
@dlt.table(name=f'cod_boost_bronze')
def boost_bronze():
    bronze_df = (spark.read
      .format("csv")
      .options(**CSV_READ_OPTIONS)
      .option("inferSchema", "true")
      .load(COUNTRY_MICRODATA_DIR)
    )
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_")
        new_col_name = ''.join(c for c in unicodedata.normalize('NFD', new_col_name) if unicodedata.category(c) != 'Mn')
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name=f'cod_boost_silver')
def boost_silver():
    return (dlt.read(f'cod_boost_bronze')
        .withColumn('Article',coalesce(col('Article'), lit('')))
        .withColumn('Sous_Article',coalesce(col('Sous_Article'), lit('')))
        .withColumn('Nature_Economique',coalesce(col('Nature_Economique'), lit('')))
        .withColumn('is_debt_repayment', col('Titre') == "1 DETTE PUBLIQUE EN CAPITAL")
        .filter(~col('is_debt_repayment'))
        .withColumn('is_foreign', (col('Source').isin('Budget General_Externe', 'Externe') & (col('Article')!='12 Dette Exterieure')))
        .withColumn('admin0', lit('Central'))
        .withColumn('admin1', lit('Central Scope'))
        .withColumn('admin2', initcap(regexp_replace(col('Chapitre'), '^[0-9\\s]*', '')))
        .withColumn('geo1',
            when(col("Province").startswith('00'), 'Central Scope')
            .when((col("Province").startswith("19") | col("Province").startswith('27')), 'Central Scope')
            .when(col("Province").isNotNull(),
                initcap(trim(regexp_replace(regexp_replace(col("Province"), "\d+", ""), "-", " ")))))
        .withColumn('func_sub',
            # education
            when(((col('Sous_Fonction_').startswith('940')) | (col('Sous_Fonction_').startswith('941'))), 'Tertiary Education')
            .when((col('Grande_Fonction').startswith('09')) & (~col('Sous_Fonction_').startswith('940')) & (~col('Sous_Fonction_').startswith('941')), 'Primary and Secondary education') # this is total education - Tertially Education        
            # Public Safety
            .when(col('Fonction2').startswith('3'), 'Public Safety' )
            # Judiciary (SHOULD come after Public Safety)
            .when(col('Fonction2').startswith('33'), 'Judiciary'))
            # No specific indicators for primary, secondary, tertiary or quaternary health
        .withColumn('func',
            when(col('Grande_Fonction').startswith('01'), 'General public services')
            .when(col('Grande_Fonction').startswith('02'), 'Defence')
            .when(col('Grande_Fonction').startswith('03'), 'Public order and safety')
            .when(col('Grande_Fonction').startswith('04'), 'Economic affairs')
            .when(col('Grande_Fonction').startswith('05'), 'Environmental protection')
            .when(col('Grande_Fonction').startswith('06'), 'Housing and community amenities')
            .when(col('Grande_Fonction').startswith('07'), 'Health')
            .when(col('Grande_Fonction').startswith('08'), 'Recreation, culture and religion')              
            .when(col('Grande_Fonction').startswith('09'), 'Education')        
            .when(col('Grande_Fonction').startswith('10'), 'Social protection'))
        .withColumn('econ_sub',
            # Basic Wages
            when((col('Exercice')<2016) &
                ((~col('Article').startswith('12')) &
                 col('Titre').startswith('3') &
                 (~col('Sous_Article').startswith('34'))), 'Basic Wages')
            .when((col('Exercice')>=2016) & 
                 ((~col('Article').startswith('12')) &
                 (col('Titre').startswith('3')) &
                 (col('Sous_Article').startswith('3661'))), 'Basic Wages')
            # Allowances
            .when((col('Exercice')<2016) &
                 ((~col('Article').startswith('12')) &
                 (col('Titre').startswith('3')) &
                 (col('Sous_Article').startswith('34'))), 'Allowances')
            .when((col('Exercice')>=2016) &
                 ((~col('Article').startswith('12')) &
                 (col('Titre').startswith('3')) &
                 (~col('Sous_Article').startswith('3661'))), 'Allowances')
            # Capital Expenditure (foreign spending)
            .when(((~col('Article').startswith('12')) &
                 ((col('Titre').startswith('7') | (col('Titre').startswith('8')))) &
                 col('is_foreign')), 'Capital Expenditure (foreign spending)')
            # Capital Maintenance
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('8233')), 'Capital Maintenance') # only post 2015
            # Basic Services                
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('5611')), 'Basic Services')# only post 2015
            # Recurrent Maintenance
            .when((col('Exercice')<2016) & (col('Sous_Article').startswith('55') | col('Sous_Article').startswith('57')), 'Recurrent Maintenance')
            .when((col('Exercice')>=2016) &
                 (col('Nature_Economique').startswith('5615') | col('Nature_Economique').startswith('5617')), 'Recurrent Maintenance')
            # Subsidies to Production
            .when((col('Exercice')<2016) & (col('Sous_Article').startswith('6150')), 'Subsidies to Production')
            .when((col('Exercice')>=2016) & (col('Nature_Economique').startswith('66413')), 'Subsidies to Production')
            # Social Assistance
            .when((col('Exercice')<2016) & 
                 (col('Grande_Fonction').startswith('10') &
                 col('Titre').startswith('6') &
                 (~col('Article').startswith('68')) &
                 (~col('Sous_Article').startswith('6350')) &
                 (~col('Sous_Article').startswith('6110')) &
                 (~col('Sous_Article').startswith('6150'))),  'Social Assistance')
            .when((col('Exercice')>=2016) &
                 col('Grande_Fonction').startswith('10') &
                 col('Titre').startswith('6') & (~col('Nature_Economique').startswith('66441')), 'Social Assistance')
             # Pensions
            .when(((col('Exercice')<2016) & col('Article').startswith('68')), 'Pensions')
            .when(((col('Exercice')>=2016) & col('Nature_Economique').startswith('66441')), 'Pensions'))
        .withColumn('econ',
            # social benefits
            when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits')
            # subsidies
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('6641 ')), 'Subsidies')
            # subsidies
            .when(((col('Exercice')<2016) & (col('Sous_Article').startswith('6110') | 
                                            col('Sous_Article').startswith('6150'))), 'Subsidies')
            # Wage bill
            .when(((~col('Article').startswith('12')) & col('Titre').startswith('3')), 'Wage bill')
            # Capital expenditure
            .when(((~col('Article').startswith('12')) &
                 ((col('Titre').startswith('7') | (col('Titre').startswith('8'))))), 'Capital expenditures')
            # Goods and services
            .when((~col('Article').startswith('12')) & (col('Titre').startswith('4') | col('Titre').startswith('5')), 'Goods and services')
            # grants and transfers
            .when((col('Exercice')<2016) & 
                 (col('Article').startswith('61') &
                 (~col('Sous_Article').startswith('6110')) &
                 (~col('Sous_Article').startswith('6150'))), 'Other grants and transfers')
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('6642')), 'Other grants and transfers')
            # interest on debt
            .when((col('Article').startswith('21') & (col('Exercice')<2016)) |
                  ((~col('Article').startswith('12')) & (col('Sous_Article').startswith('26')) & (col('Exercice')>=2016)), 'Interest on debt') # excel has a redundant condition on Article
            # other expenses
            .otherwise('Other expenses')
        )
    )

@dlt.table(name=f'cod_boost_gold')
def boost_gold():
    return (dlt.read(f'cod_boost_silver')
            .withColumn('country_name', lit('Congo, Dem. Rep.'))
            .select('country_name',
                    col('Exercice').alias('year'),
                    col('Montant_Vote').alias('approved'),
                    col('Montant_Engage').alias('revised'),
                    col('Montant_Paye').alias('executed'),
                    'admin0',
                    'admin1',
                    'admin2',
                    'geo1',
                    'is_foreign',
                    'func',
                    'func_sub',
                    'econ',
                    'econ_sub'
                    ))
