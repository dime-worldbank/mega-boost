# Databricks notebook source
import dlt
from pyspark.sql.functions import col, concat, lit, monotonically_increasing_id, regexp_replace, trim, when
from pyspark.sql.types import BooleanType, DoubleType, IntegerType

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

# Every predicate below is the SUMIFS criteria of one row of "Peru BOOST reduced.xlsx"
# (the Approved and Executed sheets share the criteria; row numbers are the Executed
# sheet's). Peru changed its classifier labels in 2009 and again in 2017 and the
# workbook switches criteria at the same years, so each helper is gated on the same
# year ranges as the sheet. Every SUMIFS in the sheet also carries the total-expenditure
# filter of row 2, which the silver table applies once up front.
# verification.md lists the lines the workbook counts in two rows at once, which a
# per-line pipeline cannot reproduce, and the precedence chosen for each.


def clean_label(column_name):
    return trim(regexp_replace(col(column_name).cast("string"), r"\s+", " "))


def strip_leading_code(column_name):
    return trim(regexp_replace(col(column_name), r"^[0-9A-Z]+\s+", ""))


def is_pre_2009():
    return col("year") <= 2008


def is_from_2009():
    return col("year") >= 2009


def is_2009_to_2016():
    return (col("year") >= 2009) & (col("year") <= 2016)


def is_from_2017():
    return col("year") >= 2017


# ---------------------------------------------------------------------------
# Functional classification (func)
# ---------------------------------------------------------------------------


def is_defence():
    # Row 28: 2006-08 function2 "66 ORDEN EXTERNO";
    #         2009+   function2 "013 DEFENSA Y SEGURIDAD NACIONAL".
    return (
        (is_pre_2009() & (col("function2") == "66 ORDEN EXTERNO"))
        | (is_from_2009() & (col("function2") == "013 DEFENSA Y SEGURIDAD NACIONAL"))
    )


def is_judiciary():
    # Row 32: 2006-08 function1 "02 JUSTICIA"; 2009+ function1 "06 JUSTICIA".
    # (The 2009+ formula also subtracts econ3 "81 AMORTIZACION DE LA DEUDA",
    # which the row 2 total filter already removes.)
    return (
        (is_pre_2009() & (col("function1") == "02 JUSTICIA"))
        | (is_from_2009() & (col("function1") == "06 JUSTICIA"))
    )


def is_public_safety():
    # Row 36: 2006-08 function2 "22 ORDEN INTERNO";
    #         2009+   function1 "05 ORDEN PUBLICO Y SEGURIDAD", function2 <> "013 DEFENSA Y SEGURIDAD NACIONAL".
    return (
        (is_pre_2009() & (col("function2") == "22 ORDEN INTERNO"))
        | (
            is_from_2009()
            & (col("function1") == "05 ORDEN PUBLICO Y SEGURIDAD")
            & (col("function2") != "013 DEFENSA Y SEGURIDAD NACIONAL")
        )
    )


def is_public_order_and_safety():
    # Row 30 = SUM(row 32, row 36).
    return is_judiciary() | is_public_safety()


# Row 41 function1 lists, quoted as the sheet has them. In the 2006-08 list,
# "11 PESCA" and "16 COMUNICACIONES" are 2009+ labels (the 2006-08 data uses
# "12 PESCA" and "06 COMUNICACIONES"), so they match nothing in those years and
# fisheries and telecoms stay in General public services there, as in the sheet.
ECONOMIC_AFFAIRS_FUNCTION1_2006_2008 = [
    "07 TRABAJO",
    "08 COMERCIO",
    "09 TURISMO",
    "04 AGRARIA",
    "11 PESCA",
    "10 ENERGIA Y RECURSOS MINERALES",
    "13 MINERIA",
    "14 INDUSTRIA",
    "16 TRANSPORTE",
    "16 COMUNICACIONES",
]
ECONOMIC_AFFAIRS_FUNCTION1_FROM_2009 = [
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
]


def is_economic_affairs():
    # Row 41: SUM(SUMIFS(function1 in list), SUMIFS(function3 "0074 VIAS URBANAS"))
    # for 2006-16; from 2017 only the function1 term.
    return (
        (is_pre_2009() & col("function1").isin(ECONOMIC_AFFAIRS_FUNCTION1_2006_2008))
        | (is_from_2009() & col("function1").isin(ECONOMIC_AFFAIRS_FUNCTION1_FROM_2009))
        | ((col("year") <= 2016) & (col("function3") == "0074 VIAS URBANAS"))
    )


def is_environment():
    # Row 193: 2006-08 function2 in {"11 PRESERVACION DE LOS RECURSOS NATURALES RENOVABLES",
    #                                "48 PROTECCION DEL MEDIO AMBIENTE"};
    #          2009-16 function1 "17 MEDIO AMBIENTE"; 2017-21 function1 "17 AMBIENTE";
    #          2022+   function1 "17 MEDIO AMBIENTE".
    # (The 2010+ formulas also subtract econ3 "81 ...", already removed by the total filter.)
    return (
        (
            is_pre_2009()
            & col("function2").isin(
                "11 PRESERVACION DE LOS RECURSOS NATURALES RENOVABLES",
                "48 PROTECCION DEL MEDIO AMBIENTE",
            )
        )
        | (is_2009_to_2016() & (col("function1") == "17 MEDIO AMBIENTE"))
        | ((col("year") >= 2017) & (col("year") <= 2021) & (col("function1") == "17 AMBIENTE"))
        | ((col("year") >= 2022) & (col("function1") == "17 MEDIO AMBIENTE"))
    )


def is_housing():
    # Row 203: 2006-08 function1 "17 VIVIENDA Y DESARROLLO URBANO", function3 <> "0074 VIAS URBANAS";
    #          2009-16 function1 "19 VIVIENDA Y DESARROLLO URBANO", function3 <> "0074 VIAS URBANAS";
    #          2017+   function1 "19 VIVIENDA Y DESARROLLO URBANO".
    return (
        (
            is_pre_2009()
            & (col("function1") == "17 VIVIENDA Y DESARROLLO URBANO")
            & (col("function3") != "0074 VIAS URBANAS")
        )
        | (
            is_2009_to_2016()
            & (col("function1") == "19 VIVIENDA Y DESARROLLO URBANO")
            & (col("function3") != "0074 VIAS URBANAS")
        )
        | (is_from_2017() & (col("function1") == "19 VIVIENDA Y DESARROLLO URBANO"))
    )


def is_health():
    # Row 215: 2006-08 function1 "14 SALUD Y SANEAMIENTO", function2 <> "47 SANEAMIENTO";
    #          2009+   function1 "20 SALUD".
    return (
        (
            is_pre_2009()
            & (col("function1") == "14 SALUD Y SANEAMIENTO")
            & (col("function2") != "47 SANEAMIENTO")
        )
        | (is_from_2009() & (col("function1") == "20 SALUD"))
    )


def is_recreation_culture():
    # Row 233: 2006-08 function2 "33*" or "34*"; 2009+ function1 "21 CULTURA Y DEPORTE".
    return (
        (is_pre_2009() & (col("function2").startswith("33") | col("function2").startswith("34")))
        | (is_from_2009() & (col("function1") == "21 CULTURA Y DEPORTE"))
    )


def is_education():
    # Row 235: 2006-08 function1 "09 EDUCACION Y CULTURA"; 2009+ function1 "22 EDUCACION".
    return (
        (is_pre_2009() & (col("function1") == "09 EDUCACION Y CULTURA"))
        | (is_from_2009() & (col("function1") == "22 EDUCACION"))
    )


def is_social_protection():
    # Row 257: 2006-08 function1 "05 ASISTENCIA Y PREVISION SOCIAL";
    #          2009+   function1 in {"23 PROTECCION SOCIAL", "24 PREVISION SOCIAL"}.
    return (
        (is_pre_2009() & (col("function1") == "05 ASISTENCIA Y PREVISION SOCIAL"))
        | (is_from_2009() & col("function1").isin("23 PROTECCION SOCIAL", "24 PREVISION SOCIAL"))
    )


# ---------------------------------------------------------------------------
# Sub-functional classification (func_sub)
# ---------------------------------------------------------------------------


def is_road():
    # Row 76: 2006-08 function2 "52 TRANSPORTE TERRESTRE" + function3 "157 VIAS URBANAS";
    #         2009+   function2 "033 TRANSPORTE TERRESTRE"
    #                 + (function1 "19 VIVIENDA Y DESARROLLO URBANO", function3 "0074 VIAS URBANAS").
    return (
        (
            is_pre_2009()
            & ((col("function2") == "52 TRANSPORTE TERRESTRE") | (col("function3") == "157 VIAS URBANAS"))
        )
        | (
            is_from_2009()
            & (
                (col("function2") == "033 TRANSPORTE TERRESTRE")
                | (
                    (col("function1") == "19 VIVIENDA Y DESARROLLO URBANO")
                    & (col("function3") == "0074 VIAS URBANAS")
                )
            )
        )
    )


def is_railroad():
    # Row 91: 2006-08 function2 "53 TRANSPORTE FERROVIARIO"; 2009+ function2 "034 TRANSPORTE FERROVIARIO".
    return (
        (is_pre_2009() & (col("function2") == "53 TRANSPORTE FERROVIARIO"))
        | (is_from_2009() & (col("function2") == "034 TRANSPORTE FERROVIARIO"))
    )


def is_water_transport():
    # Row 104: 2006-08 function2 "54 TRANSPORTE HIDROVIARIO"; 2009+ function2 "035 TRANSPORTE HIDROVIARIO".
    return (
        (is_pre_2009() & (col("function2") == "54 TRANSPORTE HIDROVIARIO"))
        | (is_from_2009() & (col("function2") == "035 TRANSPORTE HIDROVIARIO"))
    )


def is_air_transport():
    # Row 117: 2006-08 function2 "51 TRANSPORTE AEREO"; 2009+ function2 "032 TRANSPORTE AEREO".
    return (
        (is_pre_2009() & (col("function2") == "51 TRANSPORTE AEREO"))
        | (is_from_2009() & (col("function2") == "032 TRANSPORTE AEREO"))
    )


def is_agriculture():
    # Row 43: 2006-08 function1 "04 AGRARIA" + function1 "11 PESCA";
    #         2009+   function1 "10 AGROPECUARIA" + function1 "11 PESCA"
    #         (2010+ also subtracts econ3 "81 ...", already removed by the total filter).
    return (
        (is_pre_2009() & col("function1").isin("04 AGRARIA", "11 PESCA"))
        | (is_from_2009() & col("function1").isin("10 AGROPECUARIA", "11 PESCA"))
    )


def is_energy():
    # Row 130: 2006-08 function1 "10 ENERGIA Y RECURSOS MINERALES"; 2009+ function1 "12 ENERGIA".
    return (
        (is_pre_2009() & (col("function1") == "10 ENERGIA Y RECURSOS MINERALES"))
        | (is_from_2009() & (col("function1") == "12 ENERGIA"))
    )


def is_telecom():
    # Row 188: 2006-08 function1 "06 COMUNICACIONES"; 2009-16 function2 "038 TELECOMUNICACIONES";
    #          2017+   function1 "16 COMUNICACIONES".
    return (
        (is_pre_2009() & (col("function1") == "06 COMUNICACIONES"))
        | (is_2009_to_2016() & (col("function2") == "038 TELECOMUNICACIONES"))
        | (is_from_2017() & (col("function1") == "16 COMUNICACIONES"))
    )


def is_water_supply():
    # Row 205: 2006-08 function2 "47 SANEAMIENTO";
    #          2009-16 function3 in {"0088 SANEAMIENTO URBANO", "0089 SANEAMIENTO RURAL"};
    #          2017+   function1 "18 SANEAMIENTO".
    return (
        (is_pre_2009() & (col("function2") == "47 SANEAMIENTO"))
        | (is_2009_to_2016() & col("function3").isin("0088 SANEAMIENTO URBANO", "0089 SANEAMIENTO RURAL"))
        | (is_from_2017() & (col("function1") == "18 SANEAMIENTO"))
    )


def is_primary_education():
    # Row 241 (2009+): function1 "22 EDUCACION", function3 in {"0103 EDUCACION INICIAL", "0104 EDUCACION PRIMARIA"}.
    return (
        is_from_2009()
        & (col("function1") == "22 EDUCACION")
        & col("function3").isin("0103 EDUCACION INICIAL", "0104 EDUCACION PRIMARIA")
    )


def is_secondary_education():
    # Row 243 (2009+): function1 "22 EDUCACION", function3 "0105 EDUCACION SECUNDARIA".
    return is_from_2009() & (col("function1") == "22 EDUCACION") & (col("function3") == "0105 EDUCACION SECUNDARIA")


def is_tertiary_education():
    # Row 247 (2009+): function1 "22 EDUCACION",
    #                  function2 in {"048 EDUCACION SUPERIOR", "049 EDUCACION TECNICA PRODUCTIVA"}.
    return (
        is_from_2009()
        & (col("function1") == "22 EDUCACION")
        & col("function2").isin("048 EDUCACION SUPERIOR", "049 EDUCACION TECNICA PRODUCTIVA")
    )


def is_irrigation():
    # Row 52 (2009+): function1 "10 AGROPECUARIA" and one of
    #   function2 "025 RIEGO";
    #   function2 "009 CIENCIA Y TECNOLOGIA", function3 "0050 INFRAESTRUCTURA DE RIEGO";
    #   function2 "023 AGRARIO", function3 "0050 INFRAESTRUCTURA DE RIEGO" or "0051 RIEGO TECNIFICADO";
    #   function2 "027 ACUICULTURA", function3 "0050 INFRAESTRUCTURA DE RIEGO".
    # Not emitted as a func_sub: it is a subset of Agriculture (row 43) and a line
    # carries one func_sub. Kept for reference.
    return (
        is_from_2009()
        & (col("function1") == "10 AGROPECUARIA")
        & (
            (col("function2") == "025 RIEGO")
            | ((col("function2") == "009 CIENCIA Y TECNOLOGIA") & (col("function3") == "0050 INFRAESTRUCTURA DE RIEGO"))
            | ((col("function2") == "023 AGRARIO") & col("function3").isin("0050 INFRAESTRUCTURA DE RIEGO", "0051 RIEGO TECNIFICADO"))
            | ((col("function2") == "027 ACUICULTURA") & (col("function3") == "0050 INFRAESTRUCTURA DE RIEGO"))
        )
    )


# ---------------------------------------------------------------------------
# Economic classification (econ, econ_sub)
# ---------------------------------------------------------------------------


def is_wage():
    # Row 4: 2006-08 econ2 "1 PERSONAL Y OBLIGACIONES SOCIALES", econ4 <> "11 OBLIGACIONES DEL EMPLEADOR";
    #        2009-16 econ2 "1 PERSONAL Y OBLIGACIONES SOCIALES", econ3 <> "13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL";
    #        2017+   econ2 "1 PERSONAL Y OBLIGACIONES SOCIALES".
    personal = col("econ2") == "1 PERSONAL Y OBLIGACIONES SOCIALES"
    return (
        (is_pre_2009() & personal & (col("econ4") != "11 OBLIGACIONES DEL EMPLEADOR"))
        | (is_2009_to_2016() & personal & (col("econ3") != "13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL"))
        | (is_from_2017() & personal)
    )


def is_pension_contribution():
    # Row 7: 2006-08 econ4 "11 OBLIGACIONES DEL EMPLEADOR"; 2009+ econ3 "13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL".
    return (
        (is_pre_2009() & (col("econ4") == "11 OBLIGACIONES DEL EMPLEADOR"))
        | (is_from_2009() & (col("econ3") == "13 CONTRIBUCIONES A LA SEGURIDAD SOCIAL"))
    )


def is_capital():
    # Row 8: econ1 "6 GASTOS DE CAPITAL".
    return col("econ1") == "6 GASTOS DE CAPITAL"


def is_goods_services():
    # Row 11: econ2 "3 BIENES Y SERVICIOS".
    return col("econ2") == "3 BIENES Y SERVICIOS"


def is_basic_services():
    # Row 12 (2009+; ".." before): econ4 "3202 SERVICIOS BASICOS, COMUNICACIONES, PUBLICIDAD Y DIFUSION".
    return is_from_2009() & (col("econ4") == "3202 SERVICIOS BASICOS, COMUNICACIONES, PUBLICIDAD Y DIFUSION")


def is_employment_contract():
    # Row 13: econ4 "3207 SERVICIOS PROFESIONALES Y TECNICOS" + econ4 "3208 CONTRATO ADMINISTRATIVO DE SERVICIOS"
    # (the sheet applies the same codes to 2006-08, where they do not occur).
    return col("econ4").isin("3207 SERVICIOS PROFESIONALES Y TECNICOS", "3208 CONTRATO ADMINISTRATIVO DE SERVICIOS")


def is_recurrent_maintenance():
    # Row 14 (2009+; ".." before): econ4 "3204 SERVICIO DE MANTENIMIENTO, ACONDICIONAMIENTO Y REPARACIONES".
    return is_from_2009() & (col("econ4") == "3204 SERVICIO DE MANTENIMIENTO, ACONDICIONAMIENTO Y REPARACIONES")


def is_subsidy():
    # Row 15 (2009+; ".." before): econ3 in {"51 SUBSIDIOS", "52 TRANSFERENCIAS A INSTITUCIONES SIN FINES DE LUCRO"}.
    return is_from_2009() & col("econ3").isin(
        "51 SUBSIDIOS",
        "52 TRANSFERENCIAS A INSTITUCIONES SIN FINES DE LUCRO",
    )


def is_subsidy_to_production():
    # Row 16 (2009+; ".." before): econ3 "51 SUBSIDIOS".
    return is_from_2009() & (col("econ3") == "51 SUBSIDIOS")


def is_social_assistance():
    # Row 18: 2006-08 function1 "05 ASISTENCIA Y PREVISION SOCIAL", econ4 <> "14 PENSIONES";
    #         2009+   econ3 "22 PRESTACIONES Y ASISTENCIA SOCIAL".
    return (
        (is_pre_2009() & 
         (col("function1") == "05 ASISTENCIA Y PREVISION SOCIAL") & 
         (col("econ4") != "14 PENSIONES") & 
         (col("econ2") != "1 PERSONAL Y OBLIGACIONES SOCIALES") &
         (col("econ2") != "3 BIENES Y SERVICIOS") &
         (col("econ1") != "6 GASTOS DE CAPITAL"))
        | (is_from_2009() & (col("econ3") == "22 PRESTACIONES Y ASISTENCIA SOCIAL"))
    )


def is_pension():
    # Row 19: 2006-08 econ4 "14 PENSIONES"; 2009+ econ3 "21 PENSIONES".
    return (
        (is_pre_2009() & (col("econ4") == "14 PENSIONES"))
        | (is_from_2009() & (col("econ3") == "21 PENSIONES"))
    )


def is_interest():
    # Row 26: 2006-08 econ2 "78 INTERESES Y CARGOS DE LA DEUDA"; 2009+ econ3 "82 INTERESES DE LA DEUDA".
    return (
        (is_pre_2009() & (col("econ2") == "78 INTERESES Y CARGOS DE LA DEUDA"))
        | (is_from_2009() & (col("econ3") == "82 INTERESES DE LA DEUDA"))
    )


def is_debt_repayment():
    # Row 25: 2006-08 econ2 "79 AMORTIZACION DE LA DEUDA" (inside the row 2 total);
    #         2009+   econ3 "81 AMORTIZACION DE LA DEUDA" (outside the total; dropped in silver).
    return is_pre_2009() & (col("econ2") == "79 AMORTIZACION DE LA DEUDA")


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

    # 2006-2008 Raw carries the regional label in title case as well as ALL CAPS.
    # Excel's SUMIFS matches case-insensitively, so the workbook's admin1,
    # "2 GOBIERNOS REGIONALES" criteria already count both spellings; Spark
    # compares case-sensitively, so normalize to keep the same lines.
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
        # Row 2 (total expenditures): econ3 <> "81 AMORTIZACION DE LA DEUDA", econ3 <> "71*".
        .filter(~((col("econ3") == "81 AMORTIZACION DE LA DEUDA") | col("econ3").startswith("71")))
        .withColumn("admin0", when(col("admin1") == "1 GOBIERNO NACIONAL", lit("Central")).otherwise(lit("Regional")))
        .withColumn(
            "admin1_tmp",
            when(col("admin0") == "Central", lit("Central Scope")).otherwise(strip_leading_code("admin1")),
        )
        .withColumn("admin2_tmp", strip_leading_code("admin1"))
        .withColumn("geo1", when(col("admin0") == "Central", lit("Central Scope")).otherwise(col("admin1_tmp")))
        .withColumn("is_pension_contribution", is_pension_contribution())
        .withColumn("is_wage", is_wage())
        .withColumn("is_capital", is_capital())
        .withColumn("is_goods_services", is_goods_services())
        .withColumn("is_subsidy", is_subsidy())
        .withColumn("is_pension", is_pension())
        .withColumn("is_social_assistance", is_social_assistance())
        .withColumn("is_interest", is_interest())
        .withColumn("is_debt_repayment", is_debt_repayment())
        .withColumn(
            "func_sub",
            # The sub-functional rows are disjoint in the data except one case:
            # in 2011, 32 function3 "0088/0089 SANEAMIENTO" lines sit under
            # function1 "10 AGROPECUARIA" (2.4M executed) and the sheet counts them
            # in both rows 205 and 43. Water Supply is listed first so the
            # function3-coded row keeps them.
            when(is_judiciary(), lit("Judiciary"))
            .when(is_public_safety(), lit("Public Safety"))
            .when(is_road(), lit("Roads"))
            .when(is_railroad(), lit("Railroads"))
            .when(is_water_transport(), lit("Water Transport"))
            .when(is_air_transport(), lit("Air Transport"))
            .when(is_water_supply(), lit("Water Supply"))
            .when(is_agriculture(), lit("Agriculture"))
            .when(is_energy(), lit("Energy"))
            .when(is_telecom(), lit("Telecom"))
            .when(is_primary_education(), lit("Primary Education"))
            .when(is_secondary_education(), lit("Secondary Education"))
            .when(is_tertiary_education(), lit("Tertiary Education"))
        )
        .withColumn(
            "func",
            # Each function is its own workbook row; func is not derived from
            # func_sub. The order only matters where the sheet counts a line in
            # two function rows, which happens in 2006-08 only (function2-coded
            # rows under a function1 that another row claims); see verification.md:
            # Environmental protection beats Economic affairs, Health and Housing;
            # Recreation beats Education; Public order and safety beats Social
            # protection.
            when(is_defence(), lit("Defence"))
            .when(is_public_order_and_safety(), lit("Public order and safety"))
            .when(is_environment(), lit("Environmental protection"))
            .when(is_economic_affairs(), lit("Economic affairs"))
            .when(is_housing(), lit("Housing and community amenities"))
            .when(is_health(), lit("Health"))
            .when(is_recreation_culture(), lit("Recreation, culture and religion"))
            .when(is_education(), lit("Education"))
            .when(is_social_protection(), lit("Social protection"))
            # Row 24: total minus the nine rows above.
            .otherwise(lit("General public services"))
        )
        .withColumn(
            "econ_sub",
            # Disjoint by code from 2009. In 2006-08, Social Assistance (row 18) is
            # function-coded and shares its econ4 "11 OBLIGACIONES DEL EMPLEADOR"
            # lines with row 7; the pension-contribution row keeps them (76-88M/yr).
            when(col("is_pension_contribution"), lit("Social Benefits (pension contributions)"))
            .when(col("is_pension"), lit("Pensions"))
            .when(col("is_social_assistance"), lit("Social Assistance"))
            .when(is_basic_services(), lit("Basic Services"))
            .when(is_employment_contract(), lit("Employment Contracts"))
            .when(is_recurrent_maintenance(), lit("Recurrent Maintenance"))
            .when(is_subsidy_to_production(), lit("Subsidies to Production"))
        )
        .withColumn(
            "econ",
            # One econ per line. The sheet's econ rows overlap in two places (see
            # verification.md), and Other expenses (row 22 = total minus the other
            # rows) absorbs the difference:
            # - 2009+: econ3 "52 ..." lines under econ1 "6 GASTOS DE CAPITAL" are in
            #   both Subsidies (row 15) and Capital expenditures (row 8); Subsidies
            #   keeps them (0.1-0.9B/yr), so Subsidies matches the sheet and Capital
            #   is lower by that amount.
            # - 2006-08: Social assistance (row 18) is function-coded and overlaps
            #   Wage bill, Goods and services and Capital; those keep the lines
            #   (0.7-1.0B/yr), so Social benefits is lower by that amount.
            # Pension contributions are outside the sheet's Social benefits; before
            # 2017 they fall into Other expenses and from 2017 they are in Wage bill.
            when(col("is_wage"), lit("Wage bill"))
            .when(col("is_goods_services"), lit("Goods and services"))
            .when(col("is_subsidy"), lit("Subsidies"))
            .when(col("is_capital"), lit("Capital expenditures"))
            # Row 17 = SUM(rows 18:20): Social Assistance + Pensions.
            .when(col("is_pension") | col("is_social_assistance"), lit("Social benefits"))
            .when(col("is_interest"), lit("Interest on debt"))
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
            # The workbook has no foreign-funding formula (EXP_ECON_TOT_EXP_FOR_EXE and
            # every *_FOR_EXE row are ".."), so the flag is left null, as for Paraguay.
            lit(None).cast(BooleanType()).alias("is_foreign"),
            "approved",
            lit(None).cast(DoubleType()).alias("revised"),
            "executed",
        )
    )
