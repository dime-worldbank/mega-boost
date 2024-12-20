# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf, trim, regexp_replace, initcap, concat, lower, create_map
from pyspark.sql.types import StringType

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Chile'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

region_mapping = {
    "IX": "Araucanía",
    "RM": "Región Metropolitana de Santiago",
    "I": "Tarapacá",
    "II": "Antofagasta",
    "III": "Atacama",
    "IV": "Coquimbo",
    "V": "Valparaíso",
    "VI": "Libertador General Bernardo O'Higgins",
    "VII": "Maule",
    "VIII": "Biobío",
    "X": "Los Lagos",
    "XI": "Aysén",
    "XII": "Magallanes y la Antártica Chilena",
    "XIV": "Los Ríos",
    "XV": "Arica y Parinacota",
    "XVI": "Ñuble",
}
region_mapping_expr = create_map([lit(key) for pair in region_mapping.items() for key in pair])

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'chl_boost_bronze_cen')
def boost_bronze_cen():
    # Load the data from CSV
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Cen.csv')
            .withColumn("region", when(col("region").isNull(), "").otherwise(col("region").cast("string")))
            .withColumn("econ1", col("econ1").cast("string"))
            .withColumn("transfer", col("transfer").cast("string"))
            .withColumn("econ2", col("econ2").cast("string"))
            .withColumn("region", region_mapping_expr.getItem(col("region")))
            .filter(
                (
                    (col('year') < 2017) & 
                    ((lower(col('transfer')) == 'excluye consolidables') & (~col('econ2').rlike('^(32|30)'))) &
                    (
                        (~col('econ2').rlike('^(28|34)')) | 
                        (lower(col('econ3')).isin("03 intereses deuda interna",
                                           "04 intereses deuda externa", 
                                           "05 otros gastos financieros deuda interna", 
                                           "06 otros gastos financieros deuda externa"))
                    )
                ) |
                ((col('year') > 2016) & 
                 (
                    (~(lower(col('econ1')).isin('financiamiento', 'consolidable'))) | 
                    (lower(col('econ3')).isin('04 intereses deuda externa', 
                                       '06 otros gastos financieros deuda externa',
                                       '03 intereses deuda interna',
                                       '05 otros gastos financieros deuda interna'))
                    )
                 )
            )
    )

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'chl_boost_bronze_municipal')
def boost_bronze_municipal():
    df1 = (spark.read
           .format("csv")
           .options(**CSV_READ_OPTIONS)
           .option("inferSchema", "true")
           .load(f'{COUNTRY_MICRODATA_DIR}/Mun.csv')
            .withColumn("econ0", when(col("econ0").isNull(), "").otherwise(col("econ0").cast("string")))
            .withColumn("econ1", when(col("econ1").isNull(), "").otherwise(col("econ1").cast("string")))
            .withColumn("econ2", when(col("econ2").isNull(), "").otherwise(col("econ2").cast("string")))
            .withColumn("econ3", when(col("econ3").isNull(), "").otherwise(col("econ3").cast("string")))
            .withColumn("econ4", when(col("econ4").isNull(), "").otherwise(col("econ4").cast("string")))
            .withColumn("Region", when(col("Region").isNull(), "").otherwise(col("Region").cast("string")))
           .filter((col('econ0') == '2 Gasto') & (lower(col('econ1')) != 'gastos por actividades de financiacion'))
           .withColumnRenamed('accrued', 'executed')
           .withColumnRenamed('Servicio', 'service')
           .withColumnRenamed('Region', 'region')
    )
    df2 = (spark.read
           .format("csv")
           .options(**CSV_READ_OPTIONS)
           .option("inferSchema", "true")
           .load(f'{COUNTRY_MICRODATA_DIR}/Mun2.csv')
            .withColumn("ECON0", when(col("ECON0").isNull(), "").otherwise(col("ECON0").cast("string")))
            .withColumn("ECON1", when(col("ECON1").isNull(), "").otherwise(col("ECON1").cast("string")))
            .withColumn("ECON2", when(col("ECON2").isNull(), "").otherwise(col("ECON2").cast("string")))
            .withColumn("ECON3", when(col("ECON3").isNull(), "").otherwise(col("ECON3").cast("string")))
            .withColumn("ECON4", when(col("ECON4").isNull(), "").otherwise(col("ECON4").cast("string")))
            .withColumn("Region", when(col("Region").isNull(), "").otherwise(col("Region").cast("string")))
           .filter((col('ECON0') == '2 Gasto') & (lower(col('ECON1')) != 'gastos por actividades de financiacion'))
           .withColumnRenamed('ACCRUED', 'executed')
           .withColumnRenamed('Service2', 'service')
           .withColumnRenamed('Region', 'region')
    )
    df2 = df2.toDF(*[col.lower() for col in df2.columns])
    df = df1.unionByName(df2, allowMissingColumns=True).withColumn("region", region_mapping_expr.getItem(col("region")))
    return df

@dlt.table(name='chl_boost_silver')
def boost_silver():
    cen = dlt.read('chl_boost_bronze_cen'
        ).withColumn('year', col('year').cast('long')
        ).withColumn(
            'sheet', lit('Cen')
        ).withColumn(
            'admin0_tmp', lit('Central')
        ).withColumn(
            'admin1_tmp', when((col('region').isNull()) | (col('region') == ''), lit('Central Scope')).otherwise(col('region'))
        ).withColumn(
            'admin2_tmp', col('admin2')
        ).withColumn('geo1', lit('Central Scope')
        ).withColumn('interest_tmp', 
            when(lower(col('econ3')).isin('04 intereses deuda externa',
                                          '06 otros gastos financieros deuda externa',
                                          '03 intereses deuda interna',
                                          '05 otros gastos financieros deuda interna'), 1)
            .otherwise(0)
        ).withColumn('func_sub',
            when((col('admin1').rlike('^(03|10)')) & (col('interest_tmp')==0), 'judiciary')
            .when((col('admin1').startswith('05')) & (col('interest_tmp')==0), 'public safety')
            # No breakdown of education spending into primary, secondary etc
            # No breakdown of health expenditure into primary, secondary etc
            # agriculture
            .when((col('year')<2020) & (col('admin1').startswith('13') & (lower(col('admin2'))!='05 corporacion nacional forestal')), 'agriculture')
            .when((col('year')>2019) & (col('admin1').startswith('13') & (~col('admin2').startswith('1305'))), 'agriculture')
            # roads
            .when(col('road_transport')=='road', 'road')
            # railroads
            .when((col('program1').rlike('^(190102|190103)') | (lower(col('program1')).isin("02 S.Y Adm.Gral.Transporte-Empresa Ferrocarriles Del Estado".lower(),"03 TRANSANTIAGO".lower()))), 'railroads')
            # water transport
            .when(lower(col('program1')) == '06 D.G.O.P.-Direccion De Obras Portuarias'.lower(), 'water transport')
            # air transport
            .when(lower(col('program1')) == '07 D.G.O.P.-Direccion De Aeropuertos'.lower(), 'air transport')
            # energy
            .when((((col('admin1').startswith('17'))&(lower(col('admin2')).isin("04 comision chilena de energia nuclear","05 comision nacional de energia")))|
                  (col('admin1').startswith('24'))), 'energy')

        ).withColumn('func',
            when(col('admin1').startswith('11'), 'Defence')
            .when(col('func_sub').isin('judiciary', 'public safety'), "Public order and safety")
            .when((col('interest_tmp')==0) & ( 
                ((col('admin1').rlike('^(07|17)') & (~lower(col('admin2')).isin("04 comision chilena de energia nuclear", "05 comision nacional de energia")))) |
                col('func_sub').isin('agriculture', 'road', 'railroads', 'water transport', 'air transport', 'energy')             
                ), 'Economic affairs')
            .when((
                col('admin2').rlike('^(1305|2501|2502|2503)') | 
                col('admin2').isin('05 Corporacion Nacional Forestal',
                                   '03 Tercer Tribunal Ambiental',
                                   '03 Superintendencia Del Medio Ambiente',
                                   '02 Segundo Tribunal Ambiental',
                                   '02 Servicio De Evaluacion Ambiental',
                                   '01 Subsecretaria De Medio Ambiente',
                                   '01 Primer Tribunal Ambiental')
            ), 'Environmental protection')
            .when(col('admin1').startswith('18'), 'Housing and community amenities')
            .when((col('admin1').startswith('16')) & (col('interest_tmp')==0), 'Health')
            .when(col('admin1').startswith('09') & (col('interest_tmp')==0), 'Education')
            # No recreation and culture spending information
            # No infromation on social protection
            .otherwise('General public services')
        ).withColumn('econ_sub',
            # social assistance
            when(lower(col('econ3')) == '02 prestaciones de asistencia social', 'social assistance')
            # pensions
            .when(lower(col('econ3')) == '01 prestaciones previsionales', 'pensions')
        ).withColumn('econ',
            # wage bill
            when(col('econ2').startswith('21'), 'Wage bill')
            # capex
            .when(lower(col('econ1'))=='gasto de capital', 'Capital expenditures')
            # goods and services
            .when(col('econ2').startswith('22'), 'Goods and services')
            # subsidies
            .when((col('subsidies')=='y') & 
                  (lower(col('econ3')) !="02 prestaciones de asistencia social") &
                  (lower(col('econ3')) !="01 prestaciones previsionales") &
                  (lower(col('econ3')) != "03 a otras entidades publicas") &
                  (lower(col('econ1')) !="gasto de capital"), 'Subsidies')
            # social benefits
            .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
            # Other grants and trasnfers
            .when((lower(col('econ3'))=='03 a otras entidades publicas') & (lower(col('econ1'))!='gasto de capital'), 'Other grants and transfers')
            # interest on debt
            .when(lower(col('econ3')).isin("03 intereses deuda interna",
                                            "04 intereses deuda externa",
                                            "05 otros gastos financieros deuda interna",
                                            "06 otros gastos financieros deuda externa"), 'Interest on debt')
            .otherwise('Other expenses')
        )

    mun = dlt.read('chl_boost_bronze_municipal'
        ).withColumn(
            'sheet', lit('Mun')
        ).withColumn(
            'admin0_tmp', lit('Regional')
        ).withColumn(
            'admin1_tmp', col('Region')
        ).withColumn(
            'admin2_tmp', col('Municipio')
        ).withColumn('geo1', col('Region')
        ).withColumn('func',
            when(lower(col('service'))=='area de salud', 'Health')
            .when(trim(lower(col('service')))=='area de educacion', 'Education')
            .otherwise('General public services')
        ).withColumn('econ',
            # wage bill
            when(col('econ2').startswith('21'), 'Wage bill')
            # capex
            .when(lower(col('econ1'))=='gastos por actividades de inversion', 'Capital expenditures')
            # goods and services
            .when(col('econ2').startswith('22'), 'Goods and services')
            # subsidies
            .when(((lower(col('econ2'))=='24 transferencias corrientes') & (lower(col('econ3')) == '01 al sector privado')), 'Subsidies')
            # interest on debt
            .when(lower(col('econ3')).isin('03 intereses deuda interna', '05 otros gastos financieros deuda interna'), 'Interest on debt')
            .otherwise('Other expenses')
        )
    return cen.unionByName(mun, allowMissingColumns=True)

@dlt.table(name=f'chl_boost_gold')
def boost_gold():
    return (dlt.read(f'chl_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .filter(col('year') > 2008)
        .select('country_name',
                col('year').cast("integer"),
                'approved',
                col('modified').alias('revised'),
                'executed',
                'geo1',
                col('admin0_tmp').alias('admin0'),
                col('admin1_tmp').alias('admin1'),
                'admin2',
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

