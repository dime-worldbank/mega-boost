# Databricks notebook source
import dlt
from pyspark.sql.functions import col, concat, lit, monotonically_increasing_id, regexp_replace, trim, when
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = "Peru"
COUNTRY_MICRODATA_DIR = f"{WORKSPACE_DIR}/microdata_csv/{COUNTRY}"

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

STRING_COLUMNS = [
    "admin1",
    "econ1",
    "econ2",
    "econ3",
    "econ4",
    "function1",
    "function2",
    "function3",
    "source_fin1",
    "source_fin2",
]


def clean_label(column_name):
    return trim(regexp_replace(col(column_name).cast("string"), r"\s+", " "))


def strip_leading_code(column_name):
    return trim(regexp_replace(col(column_name), r"^[0-9A-Z]+\s+", ""))


def is_pre_2009():
    return col("year") <= 2008


def is_2009_to_2016():
    return (col("year") >= 2009) & (col("year") <= 2016)


def is_from_2017():
    return col("year") >= 2017


def is_irrigation():
    return (
        (col("function1") == "10 AGROPECUARIA")
        & (
            (col("function2") == "025 RIEGO")
            | ((col("function2") == "009 CIENCIA Y TECNOLOGIA") & (col("function3") == "0050 INFRAESTRUCTURA DE RIEGO"))
            | ((col("function2") == "023 AGRARIO") & col("function3").isin("0050 INFRAESTRUCTURA DE RIEGO", "0051 RIEGO TECNIFICADO"))
            | ((col("function2") == "027 ACUICULTURA") & (col("function3") == "0050 INFRAESTRUCTURA DE RIEGO"))
        )
    )


def is_road():
    # Match CCI roads coverage without double-counting a line under two road terms.
    return (
        col("function2").isin("52 TRANSPORTE TERRESTRE", "033 TRANSPORTE TERRESTRE")
        | col("function3").isin("157 VIAS URBANAS", "0074 VIAS URBANAS")
    )


def is_defence():
    # CCI EXP_FUNC_DEF_EXE
    return (
        (is_pre_2009() & (col("function2") == "66 ORDEN EXTERNO"))
        | ((col("year") >= 2009) & (col("function2") == "013 DEFENSA Y SEGURIDAD NACIONAL"))
    )


def is_judiciary():
    # CCI EXP_FUNC_JUD_EXE
    return (
        (is_pre_2009() & (col("function1") == "02 JUSTICIA"))
        | ((col("year") >= 2009) & (col("function1") == "06 JUSTICIA"))
    )


def is_public_safety():
    # CCI EXP_FUNC_PUB_SAF_EXE
    return (
        (is_pre_2009() & (col("function2") == "22 ORDEN INTERNO"))
        | (
            (col("year") >= 2009)
            & (col("function1") == "05 ORDEN PUBLICO Y SEGURIDAD")
            & (col("function2") != "013 DEFENSA Y SEGURIDAD NACIONAL")
        )
    )


def is_public_order_and_safety():
    # CCI EXP_FUNC_PUB_ORD_SAF_EXE = judiciary + public safety
    return is_judiciary() | is_public_safety()


def is_economic_affairs_function():
    # Align with CCI EXP_FUNC_ECO_REL_EXE function1 lists by period.
    return (
        (
            is_pre_2009()
            & col("function1").isin(
                "04 AGRARIA",
                "07 TRABAJO",
                "08 COMERCIO",
                "09 TURISMO",
                "10 ENERGIA Y RECURSOS MINERALES",
                "11 PESCA",
                "13 MINERIA",
                "14 INDUSTRIA",
                "16 TRANSPORTE",
                "16 COMUNICACIONES",
            )
        )
        | (
            (col("year") >= 2009)
            & col("function1").isin(
                "07 TRABAJO",
                "08 COMERCIO",
                "09 TURISMO",
                "10 AGROPECUARIA",
                "11 PESCA",
                "12 ENERGIA",
                "13 MINERIA",
                "14 INDUSTRIA",
                "15 TRANSPORTE",
                "16 COMUNICACIONES",
            )
        )
    )


def is_environment():
    # CCI EXP_FUNC_ENV_PRO_EXE. Raw label switches between "17 AMBIENTE" and
    # "17 MEDIO AMBIENTE" after 2016, so accept both from 2017 onward.
    return (
        (
            is_pre_2009()
            & col("function2").isin(
                "11 PRESERVACION DE LOS RECURSOS NATURALES RENOVABLES",
                "48 PROTECCION DEL MEDIO AMBIENTE",
            )
        )
        | ((col("year") >= 2009) & (col("year") <= 2016) & (col("function1") == "17 MEDIO AMBIENTE"))
        | (
            is_from_2017()
            & col("function1").isin("17 AMBIENTE", "17 MEDIO AMBIENTE")
        )
    )


def is_housing():
    # CCI EXP_FUNC_HOU_EXE (exclude urban roads where CCI does)
    return (
        (
            is_pre_2009()
            & (col("function1") == "17 VIVIENDA Y DESARROLLO URBANO")
            & (col("function3") != "0074 VIAS URBANAS")
            & (col("function3") != "157 VIAS URBANAS")
        )
        | (
            (col("year") >= 2009)
            & (col("year") <= 2016)
            & (col("function1") == "19 VIVIENDA Y DESARROLLO URBANO")
            & (col("function3") != "0074 VIAS URBANAS")
        )
        | (is_from_2017() & (col("function1") == "19 VIVIENDA Y DESARROLLO URBANO"))
    )


def is_health():
    return (
        (
            is_pre_2009()
            & (col("function1") == "14 SALUD Y SANEAMIENTO")
            & (col("function2") != "47 SANEAMIENTO")
        )
        | ((col("year") >= 2009) & (col("function1") == "20 SALUD"))
    )


def is_recreation_culture():
    return (
        (is_pre_2009() & (col("function2").startswith("33") | col("function2").startswith("34")))
        | ((col("year") >= 2009) & (col("function1") == "21 CULTURA Y DEPORTE"))
    )


def is_education():
    return (
        (is_pre_2009() & (col("function1") == "09 EDUCACION Y CULTURA"))
        | ((col("year") >= 2009) & (col("function1") == "22 EDUCACION"))
    )


def is_social_protection_function():
    # CCI EXP_FUNC_SOC_PRO_EXE
    return (
        (is_pre_2009() & (col("function1") == "05 ASISTENCIA Y PREVISION SOCIAL"))
        | (
            (col("year") >= 2009)
            & col("function1").isin("23 PROTECCION SOCIAL", "24 PREVISION SOCIAL")
        )
    )


def is_pension_contribution():
    return (
        (is_pre_2009() & (col("econ4") == "11 OBLIGACIONES DEL EMPLEADOR"))
        | ((col("year") >= 2009) & (col("econ3") == "13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL"))
    )


def is_pension():
    # CCI pensions row
    return (
        (is_pre_2009() & (col("econ4") == "14 PENSIONES"))
        | ((col("year") >= 2009) & (col("econ3") == "21 PENSIONES"))
    )


def is_social_assistance():
    # CCI social assistance row (broader than wages-only exclusions)
    return (
        (is_pre_2009() & (col("function1") == "05 ASISTENCIA Y PREVISION SOCIAL") & ~is_pension())
        | ((col("year") >= 2009) & (col("econ3") == "22 PRESTACIONES Y ASISTENCIA SOCIAL"))
    )


def is_wage():
    # CCI EXP_ECON_WAG_BIL_EXE changes by period
    personal = col("econ2") == "1 PERSONAL Y OBLIGACIONES SOCIALES"
    return (
        (is_pre_2009() & personal & (col("econ4") != "11 OBLIGACIONES DEL EMPLEADOR"))
        | (is_2009_to_2016() & personal & (col("econ3") != "13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL"))
        | (is_from_2017() & personal)
    )


def is_subsidy():
    # CCI includes nonprofit transfers with subsidies
    return col("econ3").isin(
        "51 SUBSIDIOS",
        "52 TRANSFERENCIAS A INSTITUCIONES SIN FINES DE LUCRO",
    )


def is_interest():
    # CCI EXP_ECON_INT_DEB_EXE: econ2 interest group before 2009, econ3 after.
    return (
        (is_pre_2009() & (col("econ2") == "78 INTERESES Y CARGOS DE LA DEUDA"))
        | ((col("year") >= 2009) & (col("econ3") == "82 INTERESES DE LA DEUDA"))
    )


@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name="per_boost_bronze")
def boost_bronze():
    df = (
        spark.read.format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f"{COUNTRY_MICRODATA_DIR}/Raw.csv")
    )

    for column_name in STRING_COLUMNS:
        df = df.withColumn(column_name, clean_label(column_name))

    # 2006-2008 Raw uses a title-case regional admin label alongside the
    # ALL-CAPS form; normalize so regional/subnational aggregates stay consistent.
    df = df.withColumn(
        "admin1",
        when(col("admin1") == "2 Gobiernos regionales", lit("2 GOBIERNOS REGIONALES")).otherwise(col("admin1")),
    )

    return (
        df.withColumn("year", col("year").cast(IntegerType()))
        .withColumn("approved", col("monto_pia").cast(DoubleType()))
        .withColumn("executed", col("monto_devengado").cast(DoubleType()))
        .withColumn("id", concat(lit("per_"), monotonically_increasing_id()))
    )


@dlt.table(name="per_boost_silver")
def boost_silver():
    return (
        dlt.read("per_boost_bronze")
        .filter(~((col("econ3") == "81 AMORTIZACION DE LA DEUDA") | col("econ3").startswith("71")))
        .withColumn("admin0", when(col("admin1") == "1 GOBIERNO NACIONAL", lit("Central")).otherwise(lit("Regional")))
        .withColumn(
            "admin1_tmp",
            when(col("admin0") == "Central", lit("Central Scope")).otherwise(strip_leading_code("admin1")),
        )
        .withColumn("admin2_tmp", strip_leading_code("admin1"))
        .withColumn("geo1", when(col("admin0") == "Central", lit("Central Scope")).otherwise(col("admin1_tmp")))
        .withColumn("is_foreign", ~col("source_fin1").startswith("1 RECURSOS ORDINARIOS"))
        .withColumn("is_pension_contribution", is_pension_contribution())
        .withColumn("is_wage", is_wage())
        .withColumn("is_capital", col("econ1") == "6 GASTOS DE CAPITAL")
        .withColumn("is_goods_services", col("econ2") == "3 BIENES Y SERVICIOS")
        .withColumn("is_subsidy", is_subsidy())
        .withColumn("is_pension", is_pension())
        .withColumn("is_social_assistance", is_social_assistance())
        .withColumn("is_interest", is_interest())
        .withColumn("is_debt_repayment", col("econ2") == "79 AMORTIZACION DE LA DEUDA")
        .withColumn(
            "func_sub",
            when(is_judiciary(), lit("Judiciary"))
            # .when(is_irrigation(), lit("Irrigation"))
            .when(is_road(), lit("Roads"))
            .when(col("function2").isin("53 TRANSPORTE FERROVIARIO", "034 TRANSPORTE FERROVIARIO"), lit("Railroads"))
            .when(col("function2").isin("035 TRANSPORTE HIDROVIARIO"), lit("Water Transport"))
            .when(col("function2").isin("51 TRANSPORTE AEREO", "032 TRANSPORTE AEREO"), lit("Air Transport"))
            .when(
                (is_pre_2009() & col("function1").isin("04 AGRARIA", "11 PESCA"))
                | ((col("year") >= 2009) & col("function1").isin("10 AGROPECUARIA", "11 PESCA")),
                lit("Agriculture"),
            )
            .when(
                (is_pre_2009() & (col("function1") == "10 ENERGIA Y RECURSOS MINERALES"))
                | ((col("year") >= 2009) & (col("function1") == "12 ENERGIA")),
                lit("Energy"),
            )
            .when(col("function1").isin("06 COMUNICACIONES", "16 COMUNICACIONES"), lit("Telecom"))
            # .when(is_health(), lit("Health"))
            # .when(is_education(), lit("Education"))
        )
        .withColumn(
            "func",
            # Environment before economic affairs: pre-2009 env is coded in
            # function2 under agraria/energia function1s that CCI also lists in
            # economic affairs. Specific env codes should win (and match CCI ENV).
            when(is_defence(), lit("Defence"))
            .when(is_public_order_and_safety(), lit("Public order and safety"))
            .when(is_environment(), lit("Environmental protection"))
            .when(
                is_economic_affairs_function()
                | col("func_sub").isin(
                    "Irrigation",
                    "Roads",
                    "Railroads",
                    "Water Transport",
                    "Air Transport",
                    "Agriculture",
                    "Energy",
                    "Telecom",
                ),
                lit("Economic affairs"),
            )
            .when(is_housing(), lit("Housing and community amenities"))
            .when(is_health(), lit("Health"))
            .when(is_recreation_culture(), lit("Recreation, culture and religion"))
            .when(is_education(), lit("Education"))
            .when(is_social_protection_function(), lit("Social protection"))
            .otherwise(lit("General public services"))
        )
        .withColumn(
            "econ_sub",
            when(col("is_pension_contribution"), lit("Social Benefits (pension contributions)"))
            .when(col("is_pension"), lit("Pensions"))
            .when(col("is_social_assistance"), lit("Social Assistance"))
            .when(col("econ4") == "3202 SERVICIOS BASICOS, COMUNICACIONES, PUBLICIDAD Y DIFUSION", lit("Basic Services"))
            .when(col("econ4").isin("3207 SERVICIOS PROFESIONALES Y TECNICOS", "3208 CONTRATO ADMINISTRATIVO DE SERVICIOS"), lit("Employment Contracts"))
            .when(col("econ4") == "3204 SERVICIO DE MANTENIMIENTO, ACONDICIONAMIENTO Y REPARACIONES", lit("Recurrent Maintenance"))
            .when(col("econ3") == "51 SUBSIDIOS", lit("Subsidies to Production"))
            .when(col("is_capital") & col("is_foreign"), lit("Capital Expenditure (foreign spending)"))
        )
        .withColumn(
            "econ",
            # Priority mirrors CCI economic SUMIFS used in quality checks.
            # - Goods/capital before social: CCI Cap/GS are code-based and inclusive;
            #   pre-2009 social assistance is function-based and overlaps those codes.
            # - Subsidies before capital: CCI SUB includes econ3 51/52 with no capital
            #   exclusion.
            # - Do not emit a separate Debt repayment econ: CCI Other residual keeps
            #   econ2 amortization (79), and quality has no Debt repayment category.
            when(col("is_wage"), lit("Wage bill"))
            .when(col("is_goods_services"), lit("Goods and services"))
            .when(col("is_subsidy"), lit("Subsidies"))
            .when(col("is_capital"), lit("Capital expenditures"))
            .when(col("is_pension") | col("is_social_assistance"), lit("Social benefits"))
            .when(col("is_interest"), lit("Interest on debt"))
            # Pension contributions are outside CCI Social benefits; before 2017 they
            # fall into Other expenses, and from 2017 they are already in Wage bill.
            .otherwise(lit("Other expenses"))
        )
    )


@dlt.table(name="per_boost_gold")
def boost_gold():
    return (
        dlt.read("per_boost_silver")
        .withColumn("country_name", lit(COUNTRY))
        .select(
            "country_name",
            "year",
            "admin0",
            col("admin1_tmp").alias("admin1"),
            col("admin2_tmp").alias("admin2"),
            "geo1",
            "func",
            "func_sub",
            "econ",
            "econ_sub",
            "is_foreign",
            "approved",
            lit(None).cast(DoubleType()).alias("revised"),
            "executed",
        )
    )

