# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, element_at, split, upper, trim, lower, regexp_replace, initcap, coalesce

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
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
        .withColumn('is_interest', col('Titre') == "1 DETTE PUBLIQUE EN CAPITAL") # this is not interest need to rename it
        .withColumn('is_foreign', (col('Source').isin('Budget General_Externe', 'Externe') & (col('Article')!='12 Dette Exterieure')))
        .withColumn('adm1_name', 
            when(trim(lower(col("Province"))) == "00 services centraux", 'Central Scope')
            .when((trim(lower(col("Province"))) == "19 multiprovince") | (trim(lower(col("Province"))) == "27 multi-province"), 'Other')
            .when(col("Province").isNotNull(),
                lower(trim(regexp_replace(col("Province"), "\d+", ""))))
            .otherwise('Other'))
        .withColumn('admin0', lit('Central'))
        .withColumn('admin1', lit('Central'))
        .withColumn('admin2', initcap(regexp_replace(col('Chapitre'), '^[0-9\\s]*', '')))
        .withColumn('geo1',
            when(col("Province").startswith('00'), 'Central Scope')
            .when((col("Province").startswith("19") | col("Province").startswith('27')), 'Central Scope')
            .when(col("Province").isNotNull(),
                initcap(trim(regexp_replace(col("Province"), "\d+", "")))))
        .withColumn('func_sub',
            # education
            # NOTE: post 2016 the code for primary education is not present. pre-primary is the only one tagged -- we assume it refers to primary as well following previous years codes
            when(col('Sous_Fonction_').startswith('091'), 'primary education')
            .when(col('Sous_Fonction_').startswith('092'), 'secondary education') 
            .when(col('Sous_Fonction_').startswith('094'), 'tertiary education')
            # public safety
            .when(col('Fonction2').startswith('03'), 'public safety' )
            # judiciary (SHOULD come after public safety)
            .when(col('Fonction2').startswith('033'), 'judiciary'))
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
            # basic wages
            when((col('Exercice')<2016) & (~col('is_interest')) &
                ((~col('Article').startswith('12')) &
                 col('Titre').startswith('3') &
                 (~col('Sous_Article').startswith('34'))), 'basic wages')
            .when((col('Exercice')>=2016) & 
                 ((~col('Article').startswith('12')) &
                 (col('Titre').startswith('3')) &
                 (col('Sous_Article').startswith('3661'))), 'basic wages')
            # allowances
            .when((col('Exercice')<2016) & (~col('is_interest')) &
                 ((~col('Article').startswith('12')) &
                 (col('Titre').startswith('3')) &
                 (col('Sous_Article').startswith('34'))), 'allowances')
            .when((col('Exercice')>=2016) & (~col('is_interest')) &
                 ((~col('Article').startswith('12')) &
                 (col('Titre').startswith('3')) &
                 (~col('Sous_Article').startswith('3661'))), 'allowances')
            # capital expenditure (foreign spending)
            .when(((~col('Article').startswith('12')) & (~col('is_interest')) &
                 ((col('Titre').startswith('7') | (col('Titre').startswith('8')))) &
                 col('is_foreign')), 'capital expenditure (foreign spending)')
            # capital maintenance
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('8233')), 'capital maintenance') # only post 2015
            # basic services                
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('5611')), 'basic services')# only post 2015
            # recurrent maintenance
            .when((col('Exercice')<2016) & (col('Sous_Article').startswith('55') | col('Sous_Article').startswith('57')), 'recurrent maintenance')
            .when((col('Exercice')>=2016) &
                 (col('Nature_Economique').startswith('5615') | col('Nature_Economique').startswith('5617')), 'recurrent maintenance')
            # subsidies to production
            .when((col('Exercice')<2016) & (col('Sous_Article').startswith('6150')), 'subsidies to production')
            .when((col('Exercice')>=2016) & (col('Nature_Economique').startswith('66413')), 'subsidies to production')
            # social assistance
            .when((col('Exercice')<2016) & 
                 (col('Grande_Fonction').startswith('10') &
                 col('Titre').startswith('6') &
                 (~col('Article').startswith('68')) &
                 (~col('Sous_Article').startswith('6350')) &
                 (~col('Sous_Article').startswith('6110')) &
                 (~col('Sous_Article').startswith('6150'))),  'social assistance')
            .when((col('Exercice')>=2016) &
                 col('Grande_Fonction').startswith('10') &
                 col('Titre').startswith('6') & (~col('Nature_Economique').startswith('66441')), 'social assistance')
             # pensions
            .when(((col('Exercice')<2016) & col('Article').startswith('68')), 'pensions')
            .when(((col('Exercice')>=2016) & col('Nature_Economique').startswith('66441')), 'pensions'))
        .withColumn('econ',
            # social benefits
            when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
            # subsidies
            .when((col('Exercice')>=2016) & (col('Sous_Article').startswith('6641 ')), 'Subsidies')
            # subsidies
            .when(((col('Exercice')<2016) & (col('Sous_Article').startswith('6110') | 
                                            col('Sous_Article').startswith('6150'))), 'Subsidies')
            # Wage bill
            .when((~col('is_interest')) & ((~col('Article').startswith('12')) & col('Titre').startswith('3')), 'Wage bill')
            # Capital expenditure
            .when(((~col('Article').startswith('12')) & (~col('is_interest')) &
                 ((col('Titre').startswith('7') | (col('Titre').startswith('8'))))), 'Capital expenditures')
            # Goods and services
            .when((~col('is_interest')) & 
                  ((~col('Article').startswith('12')) & (col('Titre').startswith('4') | col('Titre').startswith('5'))), 'Goods and services')
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
            .otherwise('Other expenses'))
        ).withColumn('geo_0.5', 
            when(col('geo1').isin('Kwilu', 'Mai-ndombe', 'Kwango', 'Bandundu'), 'Bandundu')
            .when(col('geo1').isin('Kongo Central', 'Bas-congo'), 'Bas-Congo')
            .when(col('geo1').isin('Sud-ubangi', 'Tshuapa', 'Mongala', 'Equateur', 'Nord Ubangi'), 'Equateur')
            .when(col('geo1').isin('Kasai Central', 'Kasai', 'Kasai-occidental'), 'Kasai Occidental')
            .when(col('geo1').isin('Lomami', 'Kasai-oriental', 'Sankuru'), 'Kasai Oriental')
            .when(col('geo1').isin('Tanganyika', 'Haut-lomami', 'Lualaba', 'Haut-katanga', 'Katanga'), 'Katanga')
            .when(col('geo1').isin('Ville-province De Kinshasa', 'Kinshasa'), 'Kinshasa')
            .when(col('geo1').isin('Nord-kivu'), 'Nord Kivu')
            .when(col('geo1').isin('Ituri', 'Haut-uele', 'Bas Uele', 'Tshopo'), 'Orientale')
            .when(col('geo1').isin('Maniema'), 'Maniema')
            .when(col('geo1').isin('Sud-kivu'), 'Sud Kivu')
    )

@dlt.table(name=f'cod_boost_gold')
def boost_gold():
    return (dlt.read(f'cod_boost_silver')
            .filter(~col('is_interest'))
            .withColumn('country_name', lit('Congo, Dem. Rep.'))
            .select('country_name',
                    'adm1_name',
                    col('Exercice').alias('year'),
                    col('Montant_Vote').alias('approved'),
                    col('Montant_Engage').alias('revised'),
                    col('Montant_Paye').alias('executed'),
                    'admin0',
                    'admin1',
                    'admin2',
                    'geo1',
                    '`geo_0.5`',
                    'is_interest',
                    'is_foreign',
                    'func',
                    'func_sub',
                    'econ',
                    'econ_sub'
                    ))
