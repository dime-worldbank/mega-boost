# Databricks notebook source
import dlt
from pyspark.sql.functions import col, concat, lit, monotonically_increasing_id, regexp_replace, trim, when
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Peru'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'per_boost_bronze')
def boost_bronze():
    bronze_df = (spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f'{COUNTRY_MICRODATA_DIR}/Raw.csv')
    )
    for col_name in ['admin1', 'econ1', 'econ2', 'econ3', 'econ4', 'function1', 'function2', 'function3', 'source_fin1', 'source_fin2']:
        bronze_df = bronze_df.withColumn(col_name, trim(regexp_replace(col(col_name).cast('string'), r'\s+', ' ')))
    return (bronze_df
        # 2006-2008 has the regional label in title case too. Excel's SUMIFS is case-insensitive,
        # so the workbook counts both spellings under "2 GOBIERNOS REGIONALES"
        .withColumn('admin1',
            when(col('admin1') == '2 Gobiernos regionales', '2 GOBIERNOS REGIONALES')
            .otherwise(col('admin1'))
        ).withColumn('year', col('year').cast(IntegerType())
        ).withColumn('approved', col('monto_pia').cast(DoubleType())
        ).withColumn('executed', col('monto_devengado').cast(DoubleType())
        ).withColumn('id', concat(lit('per_'), monotonically_increasing_id())
        )
    )

@dlt.table(name=f'per_boost_silver')
def boost_silver():
    # Each condition is the SUMIFS criteria of one row of "Peru BOOST reduced.xlsx" (row numbers are
    # the Executed sheet's; Approved uses the same criteria). Peru changed its classifier labels in
    # 2009 and in 2017 and the workbook switches criteria at the same years.
    # Where the workbook counts a line in two rows, the order of the when() decides which keeps it;
    # those cases and their amounts are listed in verification.md
    pre_2009 = col('year') <= 2008
    from_2009 = col('year') >= 2009
    from_2009_to_2016 = (col('year') >= 2009) & (col('year') <= 2016)
    from_2017 = col('year') >= 2017
    # social assistance (row 18), used by both econ_sub and econ.
    # Expert's comment: in 2006-2008 keep it exclusive of wage bill, goods and services and capital expenditures
    social_assistance = (
        (pre_2009 &
         (col('function1') == '05 ASISTENCIA Y PREVISION SOCIAL') &
         (col('econ4') != '14 PENSIONES') &
         (col('econ2') != '1 PERSONAL Y OBLIGACIONES SOCIALES') &
         (col('econ2') != '3 BIENES Y SERVICIOS') &
         (col('econ1') != '6 GASTOS DE CAPITAL')) |
        (from_2009 & (col('econ3') == '22 PRESTACIONES Y ASISTENCIA SOCIAL')))
    return (dlt.read(f'per_boost_bronze')
        # total expenditures (row 2)
        .filter(~((col('econ3') == '81 AMORTIZACION DE LA DEUDA') | col('econ3').startswith('71')))
        .withColumn('admin0',
            when(col('admin1') == '1 GOBIERNO NACIONAL', 'Central')
            .otherwise('Regional')
        ).withColumn('admin2_tmp',
            trim(regexp_replace(col('admin1'), r'^[0-9A-Z]+\s+', ''))
        ).withColumn('admin1_tmp',
            when(col('admin0') == 'Central', 'Central Scope')
            .otherwise(col('admin2_tmp'))
        ).withColumn('geo1', col('admin1_tmp')
        ).withColumn('func_sub',
            # judiciary (row 32)
            when((pre_2009 & (col('function1') == '02 JUSTICIA')) |
                 (from_2009 & (col('function1') == '06 JUSTICIA')), 'Judiciary')
            # public safety (row 36)
            .when((pre_2009 & (col('function2') == '22 ORDEN INTERNO')) |
                  (from_2009 & (col('function1') == '05 ORDEN PUBLICO Y SEGURIDAD') & (col('function2') != '013 DEFENSA Y SEGURIDAD NACIONAL')), 'Public Safety')
            # roads (row 76)
            .when((pre_2009 & ((col('function2') == '52 TRANSPORTE TERRESTRE') | (col('function3') == '157 VIAS URBANAS'))) |
                  (from_2009 & (
                      (col('function2') == '033 TRANSPORTE TERRESTRE') |
                      ((col('function1') == '19 VIVIENDA Y DESARROLLO URBANO') & (col('function3') == '0074 VIAS URBANAS')))), 'Roads')
            # railroads (row 91)
            .when((pre_2009 & (col('function2') == '53 TRANSPORTE FERROVIARIO')) |
                  (from_2009 & (col('function2') == '034 TRANSPORTE FERROVIARIO')), 'Railroads')
            # water transport (row 104)
            .when((pre_2009 & (col('function2') == '54 TRANSPORTE HIDROVIARIO')) |
                  (from_2009 & (col('function2') == '035 TRANSPORTE HIDROVIARIO')), 'Water Transport')
            # air transport (row 117)
            .when((pre_2009 & (col('function2') == '51 TRANSPORTE AEREO')) |
                  (from_2009 & (col('function2') == '032 TRANSPORTE AEREO')), 'Air Transport')
            # water and sanitation (row 205). Needs to be before agriculture: in 2011 some SANEAMIENTO lines sit under 10 AGROPECUARIA
            .when((pre_2009 & (col('function2') == '47 SANEAMIENTO')) |
                  (from_2009_to_2016 & col('function3').isin('0088 SANEAMIENTO URBANO', '0089 SANEAMIENTO RURAL')) |
                  (from_2017 & (col('function1') == '18 SANEAMIENTO')), 'Water Supply')
            # agriculture (row 43)
            .when((pre_2009 & col('function1').isin('04 AGRARIA', '11 PESCA')) |
                  (from_2009 & col('function1').isin('10 AGROPECUARIA', '11 PESCA')), 'Agriculture')
            # energy (row 130)
            .when((pre_2009 & (col('function1') == '10 ENERGIA Y RECURSOS MINERALES')) |
                  (from_2009 & (col('function1') == '12 ENERGIA')), 'Energy')
            # telecoms (row 188)
            .when((pre_2009 & (col('function1') == '06 COMUNICACIONES')) |
                  (from_2009_to_2016 & (col('function2') == '038 TELECOMUNICACIONES')) |
                  (from_2017 & (col('function1') == '16 COMUNICACIONES')), 'Telecom')
            # education spending decomposed (rows 241, 243, 247), no formula before 2009
            .when(from_2009 & (col('function1') == '22 EDUCACION') &
                  col('function3').isin('0103 EDUCACION INICIAL', '0104 EDUCACION PRIMARIA'), 'Primary Education')
            .when(from_2009 & (col('function1') == '22 EDUCACION') &
                  (col('function3') == '0105 EDUCACION SECUNDARIA'), 'Secondary Education')
            .when(from_2009 & (col('function1') == '22 EDUCACION') &
                  col('function2').isin('048 EDUCACION SUPERIOR', '049 EDUCACION TECNICA PRODUCTIVA'), 'Tertiary Education')
            # Not tagged: transport (row 63, parent of the four modes), irrigation (row 52, part of agriculture)
        ).withColumn('func',
            # defence (row 28)
            when((pre_2009 & (col('function2') == '66 ORDEN EXTERNO')) |
                 (from_2009 & (col('function2') == '013 DEFENSA Y SEGURIDAD NACIONAL')), 'Defence')
            # public order and safety (row 30 = judiciary + public safety)
            .when(col('func_sub').isin('Judiciary', 'Public Safety'), 'Public order and safety')
            # environmental protection (row 193). Needs to be before economic affairs, housing and health:
            # in 2006-2008 it is coded in function2 under function1 that those rows also claim
            .when((pre_2009 & col('function2').isin('11 PRESERVACION DE LOS RECURSOS NATURALES RENOVABLES', '48 PROTECCION DEL MEDIO AMBIENTE')) |
                  (from_2009_to_2016 & (col('function1') == '17 MEDIO AMBIENTE')) |
                  ((col('year') >= 2017) & (col('year') <= 2021) & (col('function1') == '17 AMBIENTE')) |
                  ((col('year') >= 2022) & (col('function1') == '17 MEDIO AMBIENTE')), 'Environmental protection')
            # economic affairs (row 41)
            #TODO This is merely following the formula in the workbook, seems to be misclassification
            #From 2006-2008, TRABAJO in the Raw Tab has the code 15 not 07. We will come back when doing end to end
            .when((pre_2009 & col('function1').isin(
                        '07 TRABAJO', '08 COMERCIO', '09 TURISMO', '04 AGRARIA', '11 PESCA', '10 ENERGIA Y RECURSOS MINERALES',
                        '13 MINERIA', '14 INDUSTRIA', '16 TRANSPORTE', '16 COMUNICACIONES')) |
                  (from_2009 & col('function1').isin(
                        '07 TRABAJO', '08 COMERCIO', '09 TURISMO', '10 AGROPECUARIA', '11 PESCA', '12 ENERGIA',
                        '13 MINERIA', '14 INDUSTRIA', '15 TRANSPORTE', '16 COMUNICACIONES')) |
                  ((col('year') <= 2016) & (col('function3') == '0074 VIAS URBANAS') & (col('function1') != '15 TRANSPORTE')), 'Economic affairs')
            # housing (row 203)
            .when((pre_2009 & (col('function1') == '17 VIVIENDA Y DESARROLLO URBANO') & (col('function3') != '0074 VIAS URBANAS')) |
                  (from_2009_to_2016 & (col('function1') == '19 VIVIENDA Y DESARROLLO URBANO') & (col('function3') != '0074 VIAS URBANAS')) |
                  (from_2017 & (col('function1') == '19 VIVIENDA Y DESARROLLO URBANO')), 'Housing and community amenities')
            # health (row 215)
            .when((pre_2009 & (col('function1') == '14 SALUD Y SANEAMIENTO') & (col('function2') != '47 SANEAMIENTO')) |
                  (from_2009 & (col('function1') == '20 SALUD')), 'Health')
            # recreation, culture and religion (row 233). Needs to be before education: in 2006-2008 it sits under 09 EDUCACION Y CULTURA
            .when((pre_2009 & (col('function2').startswith('33') | col('function2').startswith('34'))) |
                  (from_2009 & (col('function1') == '21 CULTURA Y DEPORTE')), 'Recreation, culture and religion')
            # education (row 235)
            .when((pre_2009 & (col('function1') == '09 EDUCACION Y CULTURA')) |
                  (from_2009 & (col('function1') == '22 EDUCACION')), 'Education')
            # social protection (row 257)
            .when((pre_2009 & (col('function1') == '05 ASISTENCIA Y PREVISION SOCIAL')) |
                  (from_2009 & col('function1').isin('23 PROTECCION SOCIAL', '24 PREVISION SOCIAL')), 'Social protection')
            # general public services (row 24 = total - the rows above)
            .otherwise('General public services')
        ).withColumn('econ_sub',
            # social benefits - pension contributions (row 7)
            when((pre_2009 & (col('econ4') == '11 OBLIGACIONES DEL EMPLEADOR')) |
                 (from_2009 & (col('econ3') == '13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL')), 'Social Benefits (pension contributions)')
            # pensions (row 19)
            .when((pre_2009 & (col('econ4') == '14 PENSIONES')) |
                  (from_2009 & (col('econ3') == '21 PENSIONES')), 'Pensions')
            # social assistance (row 18)
            .when(social_assistance, 'Social Assistance')
            # basic services (row 12), no formula before 2009
            .when(from_2009 & (col('econ4') == '3202 SERVICIOS BASICOS, COMUNICACIONES, PUBLICIDAD Y DIFUSION'), 'Basic Services')
            # employment contracts (row 13)
            .when(col('econ4').isin('3207 SERVICIOS PROFESIONALES Y TECNICOS', '3208 CONTRATO ADMINISTRATIVO DE SERVICIOS'), 'Employment Contracts')
            # recurrent maintenance (row 14), no formula before 2009
            .when(from_2009 & (col('econ4') == '3204 SERVICIO DE MANTENIMIENTO, ACONDICIONAMIENTO Y REPARACIONES'), 'Recurrent Maintenance')
            # subsidies to production (row 16), no formula before 2009
            .when(from_2009 & (col('econ3') == '51 SUBSIDIOS'), 'Subsidies to Production')
        ).withColumn('econ',
            # wage bill (row 4)
            when((col('econ2') == '1 PERSONAL Y OBLIGACIONES SOCIALES') & (
                    (pre_2009 & (col('econ4') != '11 OBLIGACIONES DEL EMPLEADOR')) |
                    (from_2009_to_2016 & (col('econ3') != '13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL')) |
                    from_2017), 'Wage bill')
            # goods and services (row 11)
            .when(col('econ2') == '3 BIENES Y SERVICIOS', 'Goods and services')
            # subsidies (row 15), no formula before 2009. Needs to be before capital expenditures:
            # some 52 TRANSFERENCIAS lines are under 6 GASTOS DE CAPITAL and the workbook counts them in both
            .when(from_2009 & col('econ3').isin('51 SUBSIDIOS', '52 TRANSFERENCIAS A INSTITUCIONES SIN FINES DE LUCRO'), 'Subsidies')
            # capital expenditures (row 8)
            .when(col('econ1') == '6 GASTOS DE CAPITAL', 'Capital expenditures')
            # social benefits (row 17 = social assistance + pensions). Spelled out instead of using econ_sub because
            # in 2006-2008 some social assistance lines are tagged as pension contributions in econ_sub
            .when((pre_2009 & (col('econ4') == '14 PENSIONES')) |
                  (from_2009 & (col('econ3') == '21 PENSIONES')) |
                  social_assistance, 'Social benefits')
            # interest on debt (row 26)
            .when((pre_2009 & (col('econ2') == '78 INTERESES Y CARGOS DE LA DEUDA')) |
                  (from_2009 & (col('econ3') == '82 INTERESES DE LA DEUDA')), 'Interest on debt')
            # other expenses (row 22 = total - the rows above)
            .otherwise('Other expenses')
        )
    )

@dlt.table(name=f'per_boost_gold')
def boost_gold():
    return (dlt.read(f'per_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                lit(None).cast(DoubleType()).alias('revised'),
                'executed',
                'admin0',
                col('admin1_tmp').alias('admin1'),
                col('admin2_tmp').alias('admin2'),
                'geo1',
                # the workbook has no foreign funding formula
                lit(None).cast('boolean').alias('is_foreign'),
                'func',
                'func_sub',
                'econ',
                'econ_sub'
        )
    )
