# Databricks notebook source
import dlt
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Central African Republic'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'          # BOOST workbook dump: years before 2023
RAW_COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/raw_microdata_csv/{COUNTRY}'  # DGB statement, one YYYY.csv per year from 2023
RAW_DATA_START_YEAR = 2023

CSV_READ_OPTIONS = {"header": "true", "multiline": "true", "quote": '"', "escape": '"'}
# the Expenditure sheet's columns, which CAF_extract_raw_microdata_excel_to_csv.py reproduces (plus the SIM_ba Code);
# applied by position, so `PROGRAM 1` and `Ord. PEC` get names Delta accepts (no space)
EXPENDITURE_SCHEMA = ('YEAR double, ADMIN1 string, PROGRAM1 string, ADMIN2 string, FUNCTION1 string, '
                      'ECON1T string, ECON2T string, ECON3T string, ECON4T string, SOURCE_FIN string, Source2 string, '
                      'Dotation double, `Engagé` double, `Liquidé` double, `Ordonnancé` double, ORD_PEC double, '
                      'road_m string, road string')
# EXP_FUNC_AIR_TRA: the airport projects (admin2) of the public works ministry
AIRPORT_PROJECTS = ["Réhabilitation de la piste d'attérissage de l'Aéroport de Bangui-M'Poko",
                    "Réhabilitation de la route de l'Aéroport Bangui M'Poko-PK0",
                    "Projet de sécurisation et modernisation de l'Aéroport de Bangui-M'Poko (clôture)",
                    "Réhabilitation de la route de l'Aéroport de Bangui ",
                    "Travaux de Butimage en Béton Bitumeux de PK0-Aéoroport M'Poko (FSD)"]

# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name='caf_2022_and_before_boost_bronze')
def boost_2022_and_before_bronze():
    return (spark.read.format("csv").options(**CSV_READ_OPTIONS).schema(EXPENDITURE_SCHEMA)
        .load(f'{COUNTRY_MICRODATA_DIR}/Expenditure.csv')
        .filter(col('YEAR') < RAW_DATA_START_YEAR)
    )


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name='caf_2023_onward_boost_bronze')
def boost_2023_onward_bronze():
    return (spark.read.format("csv").options(**CSV_READ_OPTIONS).schema(f'{EXPENDITURE_SCHEMA}, Code string')
        .load(f'{RAW_COUNTRY_MICRODATA_DIR}/????.csv')
    )



# COMMAND ----------

@dlt.table(name='caf_expenditure_silver')
def expenditure_silver():
    # Every category is the SUMIFS criterion of the workbook's Executed sheet (the EXP_* code in the comment), on the
    # labels as written there (Excel compares text case-insensitively; the data is consistent). Sub-categories come
    # first and then the sheet order, so a line takes the first category that fits; a residual the sheet defines by
    # subtraction (transport, primary and secondary education, other expenses, general public services) comes after
    # what it is net of. For 2016-2021 the workbook also adds 6% (WAG_BIL, USE_GOO_SER) and 88% (CAP_EXP) of the
    # foreign-funded lines (source <> treasury) to those totals; a share cannot be given to whole lines and is not
    # reproduced.
    return (dlt.read('caf_2022_and_before_boost_bronze')
        .unionByName(dlt.read('caf_2023_onward_boost_bronze'), allowMissingColumns=True)
        .withColumn('YEAR', col('YEAR').cast(IntegerType())
        # 2022, the SIM_ba transition year, has no formula in the workbook ('..')
        ).filter(col('YEAR') != 2022
        # blank labels become '' so that a negated criterion is true for them, as Excel's "<>" is
        ).na.fill('', subset=['ADMIN2', 'PROGRAM1', 'ECON3T']
        # the workbook's total expenditure is SUMIFS(econ3, "<>pri*"): debt principal is out of scope
        ).filter(~col('ECON3T').startswith('Pri')
        ).withColumn('econ_sub',
            # pension contributions (PEN_CON)
            when(col('ECON3T') == "664 Cotisations et contributions sociales à la charge de l’Etat", 'Social Benefits (pension contributions)')
            # basic services (GOO_SER_BAS_SER)
            .when((col('ECON1T') == "3 Depenses de biens et services")
                  & (col('ECON3T') == "605 Eau, Electricité, Gaz et autres sources d’énergies "), 'Basic Services')
            # employment contracts (GOO_SER_EMP_CON)
            .when((col('ECON1T') == "3 Depenses de biens et services")
                  & (col('ECON3T') == "613 Honoraires et études "), 'Employment Contracts')
            # recurrent maintenance (REC_MAI)
            .when(col('ECON3T').startswith("614 Entretien et maintenance"), 'Recurrent Maintenance')
            # social assistance (SOC_ASS)
            .when(col('ECON3T') == "645 Transferts aux ménages", 'Social Assistance')
            # pensions (SOC_BEN_PEN)
            .when(col('ECON3T') == "648 Pensions de retraite des fonctionnaires et agents de l'Etat", 'Pensions')
        ).withColumn('econ',
            # social benefits (SOC_BEN = SOC_ASS + SOC_BEN_PEN)
            when(col('econ_sub').isin('Social Assistance', 'Pensions'), 'Social benefits')
            # goods and services (USE_GOO_SER, REC_MAI)
            .when((col('ECON1T') == "3 Depenses de biens et services") | (col('econ_sub') == 'Recurrent Maintenance'), 'Goods and services')
            # wage bill (WAG_BIL; PEN_CON is part of it)
            .when(col('ECON1T') == "2 Depenses de Personnel", 'Wage bill')
            # capital expenditures (CAP_EXP)
            .when(col('ECON1T') == "5 Depenses d'investissement", 'Capital expenditures')
            # subsidies (SUB)
            .when(col('ECON2T') == "63 SUBVENTIONS", 'Subsidies')
            # other grants and transfers (OTH_GRA = OTH_MED_RIG_EXP)
            .when(col('ECON3T') == "643 Transferts aux autres administrations publiques", 'Other grants and transfers')
            # interest on debt (INT_DEB)
            .when((col('ECON1T') == "1 Charges Financieres de la Dette") & col('ECON3T').startswith("Int"), 'Interest on debt')
            # other expenses (OTH_EXP): what is left of total expenditure after the seven categories above
            .otherwise('Other expenses')
        # admin1 is the budget chapter, "NN MINISTERE ..."; two workbook labels carry non-breaking spaces (\xa0),
        # which Excel's "=" does not equate with spaces
        ).withColumn('func_sub',
            # judiciary (JUD)
            when(col('ADMIN1').substr(1, 2).isin(['04', '08', '13']), 'Judiciary')
            # public safety (PUB_SAF)
            .when(col('ADMIN1').substr(1, 2).isin(['23', '25']), 'Public Safety')
            # agriculture (AGR)
            .when(col('ADMIN1').substr(1, 2).isin(['92', '93'])
                  | (col('ADMIN1') == "94 MINISTERE DES EAUX, FORETS, CHASSE ET PECHE"), 'Agriculture')
            # roads (ROA): the public works ministry, except its airport projects
            .when(col('ADMIN1').isin("81 MINISTERE DES TRAVAUX PUBLICS ET DE L'ENTRETIEN ROUTIER",
                                     "81 MINISTERE\xa0DE\xa0L'EQUIPEMENT\xa0ET\xa0DES\xa0TRAVAUX\xa0PUBLICS, DE L'EQUIPEMENT ET DE L'AMENAGEMENT DU TERRITOIRE")
                  & ~col('ADMIN2').isin(AIRPORT_PROJECTS), 'Roads')
            # air transport (AIR_TRA)
            .when(col('ADMIN2').isin(AIRPORT_PROJECTS), 'Air Transport')
            # transport (TRA; chapter 99 in 2016 only): what is left of it after roads and air transport
            .when(col('ADMIN1').substr(1, 2).isin(['80', '81'])
                  | ((col('YEAR') == 2016) & col('ADMIN1').startswith('99')), 'Transport')
            # energy (ENE): the energy ministries, outside the water programme
            .when((col('PROGRAM1') != 'eau')
                  & (col('ADMIN1').startswith('85')
                     | col('ADMIN1').isin("84 MINISTERE DU DEVELOPPEMENT DE L'ENERGIE ET DES RESSOURCES HYDRAULIQUES",
                                          "MINISTERE\xa0DE\xa0L'ENERGIE\xa0ET\xa0DE L'HYDRAULIQUE",
                                          "MINISTRE DES MINES, DE L'ENERGIE ET DE L'HYDRALIQUE",
                                          "MINISTRE DU DEVELOPPEMENT DE L'ENERGIE ET DES RESSOURCES HYDRAULIQUES")), 'Energy')
            # telecom (TEL)
            .when(col('ADMIN1').substr(1, 2).isin(['50', '52']) | (col('ADMIN1') == '88'), 'Telecom')
            # water supply (WAT_SAN): the water and sanitation programmes
            .when(col('PROGRAM1').isin("Appui à l'Agence Nationale de l'Eau et de l'Assainissement (ANEA)",
                                        "Programme approvisionnement en eau et assainissement en milieu rural",
                                        "Développement du système d'information sur l'eau et l'assainissement",
                                        "Appui à l'Agence de Régulation du Secteur de l'Eau et d'Assainissement (ARSEA)",
                                        "eau"), 'Water Supply')
            # tertiary education (TER_EDU): the higher education ministry, or the university services of the joint ministry
            .when((col('ADMIN1') == "40 MINISTERE DE L'ENSEIGNEMENT SUPERIEUR ET DE LA RECHERCHE SCIENTIFIQUE")
                  | col('ADMIN1').startswith('46')
                  | ((col('ADMIN1') == "40 MINISTERE DE L'EDUCATION NATIONALE, DE L'ENSEIGNEMENT SUPERIEUR ET DE LA RECHERCHE SCIENTIFIQUE")
                     & (col('ADMIN2').substr(1, 2).isin(['56', '57'])
                        | col('ADMIN2').isin("510000 Rectorat de l'Université de Bangui",
                                             "590000 Dépenses non réparties de l'ensemble des établissements universitaires",
                                             "270005 Conseil Africain et Malgache de l'Enseignement Supérieur (CAMES)",
                                             "320024 Direction des Œuvres Universitaires"))), 'Tertiary Education')
            # primary and secondary education (PRI_SEC_EDU = EDU - TER_EDU): the rest of the education chapters
            .when(col('ADMIN1').startswith('4'), 'Primary and Secondary education')
        ).withColumn('func',
            # public order and safety (PUB_ORD_SAF = JUD + PUB_SAF)
            when(col('func_sub').isin('Judiciary', 'Public Safety'), 'Public order and safety')
            # economic affairs (AGR, ROA, AIR_TRA, TRA, ENE, TEL)
            .when(col('func_sub').isin('Agriculture', 'Roads', 'Air Transport', 'Transport', 'Energy', 'Telecom'), 'Economic affairs')
            # housing and community amenities (WAT_SAN)
            .when(col('func_sub') == 'Water Supply', 'Housing and community amenities')
            # education (TER_EDU, PRI_SEC_EDU)
            .when(col('func_sub').isin('Tertiary Education', 'Primary and Secondary education'), 'Education')
            # defence (DEF)
            .when(col('ADMIN1').startswith('21'), 'Defence')
            # economic affairs (ECO_REL): chapters 90-93, 8x, 50 and 52, except 82, 84 and the "83 MINISTERE DE" ones
            .when((col('ADMIN1').substr(1, 2).isin(['90', '91', '92', '93', '50', '52']) | col('ADMIN1').startswith('8'))
                  & ~col('ADMIN1').substr(1, 2).isin(['82', '84'])
                  & ~col('ADMIN1').startswith('83 MINISTERE DE '), 'Economic affairs')
            # environmental protection (ENV_PRO = ENV_PRO_NEC)
            .when((col('ADMIN1').substr(1, 2).isin(['96', '84'])
                   | (col('ADMIN1') == "82 MINISTERE DE L'ENVIRONNEMENT ET DU DEVELOPPEMENT DURABLE"))
                  & ~col('ADMIN1').isin("96 MINISTERE DES EAUX, FORETS, CHASSE ET PECHE",
                                        "84 MINISTERE DU DEVELOPPEMENT DE L'ENERGIE ET DES RESSOURCES HYDRAULIQUES"), 'Environmental protection')
            # housing and community amenities (HOU)
            .when(col('ADMIN1').substr(1, 2).isin(['82', '83'])
                  & ~col('ADMIN1').isin("82 MINISTERE DE L'ENVIRONNEMENT ET DU DEVELOPPEMENT DURABLE",
                                        "83 MINISTERE DES MINES ET DE LA GEOLOGIE"), 'Housing and community amenities')
            # health (HEA)
            .when(col('ADMIN1').startswith('6'), 'Health')
            # recreation, culture and religion (REV_CUS_EXC)
            .when((col('ADMIN1').startswith('98') | col('ADMIN1').startswith('5'))
                  & ~col('ADMIN1').substr(1, 2).isin(['50', '52']), 'Recreation, culture and religion')
            # social protection (SOC_PRO)
            .when(col('ADMIN1').startswith('7')
                  | (col('ECON3T') == "648 Pensions de retraite des fonctionnaires et agents de l'Etat"), 'Social protection')
            # general public services (GEN_PUB_SER): what is left of total expenditure after the nine functions above
            .otherwise('General public services')
        )
    )

# COMMAND ----------

@dlt.table(name='caf_boost_gold')
def boost_gold():
    # all central government, as Burkina Faso: the budget chapter (ministry) is admin2
    return (dlt.read('caf_expenditure_silver')
        .select(lit(COUNTRY).alias('country_name'),
                col('YEAR').alias('year'),
                lit('Central').alias('admin0'),
                lit('Central Scope').alias('admin1'),
                col('ADMIN1').alias('admin2'),
                lit('Central').alias('geo0'),
                lit('Central Scope').alias('geo1'),
                # foreign funded (TOT_EXP_FOR): source <> treasury; no formula from 2023, the source is not carried over
                when(col('YEAR') < RAW_DATA_START_YEAR, col('SOURCE_FIN') != 'Treasury').alias('is_foreign'),
                'func', 'func_sub', 'econ', 'econ_sub',
                col('Dotation').cast(DoubleType()).alias('approved'),
                lit(None).cast(DoubleType()).alias('revised'),
                col('Ordonnancé').cast(DoubleType()).alias('executed'))
    )
