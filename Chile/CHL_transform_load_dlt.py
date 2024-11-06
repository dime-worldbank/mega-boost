# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import substring, col, lit, when, udf, trim, regexp_replace, initcap, concat, lower
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

@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name=f'chl_boost_bronze_cen')
def boost_bronze_cen():
    # Load the data from CSV
    return (spark.read
            .format("csv")
            .options(**CSV_READ_OPTIONS)
            .option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Cen.csv')
            .withColumn("econ1", col("econ1").cast("string"))
            .withColumn("transfer", col("transfer").cast("string"))
            .withColumn("econ2", col("econ2").cast("string"))
            .filter(
                ((col('year')<2017) & (lower(col('transfer'))=='excluye consolidables') & (~col('econ2').rlike('^(28|32|34)'))) |
                ((col('year')>2016) & (~(col('econ1').isin('Financiamiento', 'Consolidable'))))
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
            .withColumn("econ0", col("econ0").cast("string"))
            .withColumn("econ1", col("econ1").cast("string"))
            .withColumn("econ2", col("econ2").cast("string"))
            .withColumn("econ3", col("econ3").cast("string"))
            .withColumn("econ4", col("econ4").cast("string"))
           .filter((col('econ0') == '2 Gasto') & (lower(col('econ1')) != 'gastos por actividades de financiacion'))
           .withColumnRenamed('accrued', 'executed')
    )
    df2 = (spark.read
           .format("csv")
           .options(**CSV_READ_OPTIONS)
           .option("inferSchema", "true")
           .load(f'{COUNTRY_MICRODATA_DIR}/Mun2.csv')
            .withColumn("ECON0", col("ECON0").cast("string"))
            .withColumn("ECON1", col("ECON1").cast("string"))
            .withColumn("ECON2", col("ECON2").cast("string"))
            .withColumn("ECON3", col("ECON3").cast("string"))
            .withColumn("ECON4", col("ECON4").cast("string"))
           .filter((col('ECON0') == '2 Gasto') & (lower(col('ECON1')) != 'gastos por actividades de financiacion'))
           .withColumnRenamed('ACCRUED', 'executed')
    )
    df2 = df2.toDF(*[col.lower() for col in df2.columns])
    df = df1.unionByName(df2, allowMissingColumns=True)
    return df

@dlt.table(name='chl_boost_silver')
def boost_silver():
    cen = dlt.read('chl_boost_bronze_cen'
        ).withColumn('year', col('year').cast('long')
        ).withColumn(
            'sheet', lit('Cen')
        ).withColumn(
            'admin0', lit('Central')
        ).withColumn(
            'admin1', lit('Central Scope')
        ).withColumn('geo1', lit('')
        ).withColumn('func_sub',
            when(col('admin1').rlike('^(03|10)'), 'judiciary')
            .when(col('admin1').startswith('05'), 'public safety')
            # No breakdown of education spending into primary, secondary etc
            # No breakdown of health expenditure into primary, secondary etc
            # agriculture
            .when((col('admin1').startswith('13') & (lower(col('admin2'))!='05 corporacion nacional forestal')), 'agriculture')
            # railroads
            .when((col('program1').rlike('^(190102|190103)') | (col('program1').isin("02 S.Y Adm.Gral.Transporte-Empresa Ferrocarriles Del Estado","03 TRANSANTIAGO"))), 'railroads')
            # water transport
            .when(col('program1') == '06 D.G.O.P.-Direccion De Obras Portuarias', 'water transport')
            # air transport
            .when(col('program1') == '07 D.G.O.P.-Direccion De Aeropuertos', 'air transport')
        ).withColumn('func',
            when(col('admin1').startswith('11'), 'Defence')
            .when(col('func_sub').isin('judiciary', 'public safety'), "Public order and safety")
            .when((
                col('admin1').rlike('^(07|17)') |
                col('func_sub').isin('agriculture', 'railroads', 'water transport', 'air transport') |
                (col('road_transport')=='road')
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
            .when(col('admin1').startswith('16'), 'Health')
            .when(col('admin1').startswith('09'), 'Education')
            # No recreation and culture spending information
            # No infromation on social protection
            .otherwise('General public services')
        ).withColumn('econ_sub',
            # social assistance
            when(lower(col('econ3')) == '02 prestaciones de asistencia social', 'social assistance')
            # pensions
            .when(lower(col('econ3')) == '01 Prestaciones Previsionales', 'pensions')
        ).withColumn('econ',
            # wage bill
            when(col('econ2').startswith('21'), 'Wage bill')
            # capex
            .when(col('econ1')=='Gasto De Capital', 'Capital expenditures')
            # goods and services
            .when(col('econ2').startswith('22'), 'Goods and services')
            # subsidies
            .when(col('subsidies')=='y', 'Subsidies')
            # social benefits
            .when(col('econ_sub').isin('social assistance', 'pensions'), 'Social benefits')
            # Other grants and trasnfers
            .when(lower(col('econ3'))=='03 a otras entidades publicas', 'Other grants and transfers')
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
            'admin0', lit('Regional')
        ).withColumn('geo1', lit('')
        ).withColumn('func',
            when(lower(col('service2'))=='area de salud', 'Health')
            .when(trim(lower(col('service2')))=='area de educacion', 'Education')
        ).withColumn('econ',
            # wage bill
            when(col('econ2').startswith('21'), 'Wage bill')
            # capex
            .when(lower(col('econ1'))=='gastos por actividades de inversion', 'Capital expenditures')
            # goods and services
            .when(col('econ2').startswith('22'), 'Goods and services')
            # subsidies
            .when(((col('econ2')=='24 transferencias corrientes') & (col('econ3') == '01 al sector privado')), 'Subsidies')
            # interest on debt
            .when(lower(col('econ3')).isin('03 intereses deuda interna', '05 otros gastos financieros deuda interna'), 'Interest on debt')
        )
    return cen.unionByName(mun, allowMissingColumns=True)

@dlt.table(name=f'chl_boost_gold')
def boost_gold():
    return (dlt.read(f'chl_boost_silver')
        .withColumn('country_name', lit(COUNTRY))
        .select('country_name',
                'year',
                'approved',
                col('modified').alias('revised'),
                col('executed').cast('int'),
                'geo1',
                'admin0',
                'admin1',
                'admin2',
                'func_sub',
                'func',
                'econ_sub',
                'econ')
    )

