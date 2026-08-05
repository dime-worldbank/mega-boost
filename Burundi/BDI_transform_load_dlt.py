# Databricks notebook source
import re

import dlt
from pyspark.sql.functions import (
    coalesce,
    col,
    expr,
    initcap,
    lit,
    lower,
    regexp_replace,
    trim,
    when,
)
from pyspark.sql.types import DoubleType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = "Burundi"
COUNTRY_MICRODATA_DIR = f"{WORKSPACE_DIR}/microdata_csv/{COUNTRY}"
COUNTRY_MICRODATA_FILE = f"{COUNTRY_MICRODATA_DIR}/Expenditure.csv"

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}

# These codes occur in the 2013-15 Water & sanitation leaf. Keeping them in one
# unique list prevents admin2 = 42503 from being added twice.
WATER_ADMIN2_CODES_2013_15 = (
    "44012",
    "44013",
    "44014",
    "42011",
    "44503",
    "44510",
    "42503",
)

HOUSING_ADMIN2_CODES_2013_15 = (
    "45017",
    "09018",
    "45016",
    "44011",
    "44008",
    "45013",
    "11019",
)


def starts_with_any(column_name, prefixes):
    """Build one Spark predicate from a unique collection of code prefixes."""
    predicate = lit(False)
    for prefix in dict.fromkeys(prefixes):
        predicate = predicate | col(column_name).startswith(prefix)
    return predicate


def normalized_text(column_name):
    return lower(
        regexp_replace(
            trim(coalesce(col(column_name), lit(""))),
            "’",
            "'",
        )
    )


@dlt.expect_or_drop("year_not_null", "Year IS NOT NULL")
@dlt.table(name="bdi_boost_bronze")
def boost_bronze():
    bronze_df = (
        spark.read.format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(COUNTRY_MICRODATA_FILE)
    )
    for old_col_name in bronze_df.columns:
        new_col_name = re.sub(r"[ ,;{}()\n\t=]+", "_", old_col_name).strip("_")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df

@dlt.table(name="bdi_boost_silver")
def boost_silver():
    df = (
        dlt.read("bdi_boost_bronze")
        .withColumn("Year", col("Year").cast("int"))
        .withColumn("Econ_1", coalesce(col("Econ_1"), lit("")))
        .withColumn("Econ_2", coalesce(col("Econ_2"), lit("")))
        .withColumn("Econ_3", coalesce(col("Econ_3"), lit("")))
        .withColumn("Econ_4", coalesce(col("Econ_4"), lit("")))
        .withColumn("func1", coalesce(col("func1"), lit("")))
        .withColumn("func2", coalesce(col("func2"), lit("")))
        .withColumn("func3", coalesce(col("func3"), lit("")))
        .withColumn("Admin_1", coalesce(col("Admin_1"), lit("")))
        .withColumn("Admin_2", coalesce(col("Admin_2"), lit("")))
        .withColumn("Geo", coalesce(col("Geo"), lit("")))
        .withColumn("road", coalesce(col("road"), lit("")))
        .withColumn("environment", coalesce(col("environment"), lit("")))
        # The BOOST workbook excludes debt-principal repayment from the audit
        # categories. Apply that rule once, before any classification.
        .filter(~col("Econ_1").startswith("9 "))
        .withColumn("admin0", lit("Central"))
        .withColumn("admin1", lit("Central Scope"))
        .withColumn(
            "admin2",
            initcap(trim(regexp_replace(col("Admin_1"), "^[0-9\\s]*", ""))),
        )
        .withColumn(
            "geo1",
            when(
                (col("Geo") == "")
                | col("Geo").startswith("00")
                | lower(col("Geo")).contains("n/a"),
                "Central Scope",
            ).otherwise(
                initcap(trim(regexp_replace(col("Geo"), "^[0-9\\s]*", "")))
            ),
        )
        .withColumn(
            "geo1",
            when(col("geo1") == "Bujumbura Mairie", "Mairie de Bujumbura")
            .when(col("geo1") == "Bujumbura Rural", "Bujumbura")
            .otherwise(col("geo1")),
        )
    )

    year = col("Year")
    is_2013_15 = year.isin(2013, 2014, 2015)
    is_2016_17 = year.isin(2016, 2017)
    is_2019_24 = year.isin(2019, 2020, 2021, 2022, 2023, 2024)

    is_social_benefit_code = starts_with_any("Econ_3", ("616", "672", "673"))
    is_wage_bill = normalized_text("Econ_1").isin(
        "1 rémunérations des salariés",
        "1 remunerations des salaries",
    ) | (
        col("Econ_4") == "6212 Stage de premier emploi pour 250 jeunes"
    )
    is_goods_and_services = col("Econ_1").startswith("2 ")

    # Social protection owns all intersections driven by 616/672/673. For
    # 2016-17, the workbook uses COFOG 710 instead of the economic codes.
    is_social_protection = (
        (is_2016_17 & col("func1").startswith("710"))
        | ((is_2013_15 | is_2019_24) & is_social_benefit_code)
    )

    is_water_and_sanitation = (
        is_2013_15
        & (
            starts_with_any("Admin_2", WATER_ADMIN2_CODES_2013_15)
            | normalized_text("Econ_4").isin(
                "2132 réseaux adduction d'eau potable",
                "2132 reseaux adduction d'eau potable",
            )
        )
    ) | ((is_2016_17 | is_2019_24) & col("func2").startswith("7062"))

    is_housing = (
        is_2013_15
        & (
            is_water_and_sanitation
            | starts_with_any("Admin_2", HOUSING_ADMIN2_CODES_2013_15)
        )
    ) | ((is_2016_17 | is_2019_24) & col("func1").startswith("706"))

    is_defence = col("Admin_1").startswith("13 ")
    is_public_order = (
        ((year == 2016) & col("func1").startswith("703"))
        | (
            (is_2013_15 | (year == 2017) | is_2019_24)
            & starts_with_any("Admin_1", ("74", "75", "76", "16 ", "11 "))
        )
    )
    is_economic_affairs = (
        (
            is_2013_15
            & starts_with_any("Admin_1", ("40 ", "45 ", "42 ", "18 ", "41"))
        )
        | (
            (year == 2016)
            & (
                starts_with_any("func2", ("7042", "7043", "7045", "7046"))
                | col("Admin_1").startswith("41")
            )
        )
        | (
            ((year == 2017) | is_2019_24)
            & (
                col("Admin_1").startswith("40 ")
                | starts_with_any("func2", ("7043", "7045", "7046"))
                | col("Admin_1").startswith("41")
            )
        )
    )
    is_environment = (
        (is_2013_15 & (lower(col("environment")) == "y"))
        | ((is_2016_17 | is_2019_24) & col("func1").startswith("705"))
    )
    is_health = (
        (
            is_2013_15
            & (
                col("Admin_1").startswith("33 ")
                | normalized_text("Econ_4").isin(
                    "2133 réseaux d'assainissement",
                    "2133 reseaux d'assainissement",
                )
            )
        )
        | ((is_2016_17 | is_2019_24) & col("func1").startswith("707"))
    )
    is_recreation = (
        (is_2013_15 & col("Admin_1").startswith("37 "))
        | ((is_2016_17 | is_2019_24) & col("func1").startswith("708"))
    )
    is_education = (
        (is_2013_15 & starts_with_any("Admin_1", ("31 ", "32 ")))
        | ((is_2016_17 | is_2019_24) & col("func1").startswith("709"))
    )

    df = (
        df.withColumn("_is_water_and_sanitation", is_water_and_sanitation)
        .withColumn("_is_social_protection", is_social_protection)
        .withColumn("_is_housing", is_housing)
        .withColumn("_is_defence", is_defence)
        .withColumn("_is_public_order", is_public_order)
        .withColumn("_is_economic_affairs", is_economic_affairs)
        .withColumn("_is_environment", is_environment)
        .withColumn("_is_health", is_health)
        .withColumn("_is_recreation", is_recreation)
        .withColumn("_is_education", is_education)
        .withColumn(
            "func_sub",
            when(col("func2").startswith("7033"), "Judiciary")
            .when(col("func1").startswith("703"), "Public Safety")
            .when(col("func2").startswith("7042"), "Agriculture")
            .when(col("func3").startswith("70451") | (lower(col("road")) == "y"), "Roads")
            .when(
                col("func3").startswith("70453")
                | starts_with_any("Admin_2", ("45513", "45523", "41515", "41523")),
                "Railroads",
            )
            .when(
                col("func3").startswith("70454") | col("Econ_4").startswith("2136"),
                "Air Transport",
            )
            .when(col("func2").startswith("7043"), "Energy")
            .when(col("func2").startswith("7046"), "Telecom")
            # One reusable predicate implements the complete Water rule. The
            # admin2 42503 criterion appears exactly once.
            .when(col("_is_water_and_sanitation"), "Water Supply")
            .when(col("func2").startswith("7091"), "Primary Education")
            .when(col("func2").startswith("7092"), "Secondary Education")
            .when(col("func2").startswith("7094"), "Tertiary Education")
            # Fallbacks for years without complete COFOG subfunctions.
            .when(
                starts_with_any("Admin_1", ("74", "75", "76"))
                | lower(col("Admin_1")).contains("justice"),
                "Judiciary",
            )
            .when(lower(col("Admin_1")).contains("securite publique"), "Public Safety")
            .when(lower(col("Admin_1")).contains("agriculture"), "Agriculture")
            .when(
                lower(col("Admin_1")).contains("transport")
                | lower(col("Admin_1")).contains("travaux publics"),
                "Transport",
            )
            .when(lower(col("Admin_1")).contains("energie"), "Energy")
            .when(
                lower(col("Admin_1")).contains("postes")
                | lower(col("Admin_1")).contains("communication"),
                "Telecom",
            )
            .when(
                lower(col("Admin_1")).contains("enseignement superieur"),
                "Tertiary Education",
            )
            .when(
                lower(col("Admin_1")).contains("education")
                | lower(col("Admin_1")).contains("enseignement"),
                "Primary and Secondary education",
            ),
        )
        .withColumn(
            "func",
            # Ownership precedence resolves every confirmed functional overlap:
            # Social protection wins first; Housing wins over Economic affairs.
            when(col("_is_social_protection"), "Social protection")
            .when(col("_is_housing"), "Housing and community amenities")
            .when(col("_is_defence"), "Defence")
            .when(col("_is_public_order"), "Public order and safety")
            .when(col("_is_environment"), "Environmental protection")
            .when(col("_is_health"), "Health")
            .when(col("_is_recreation"), "Recreation, culture and religion")
            .when(col("_is_education"), "Education")
            .when(col("_is_economic_affairs"), "Economic affairs")
            .otherwise("General public services"),
        )
        .withColumn(
            "econ_sub",
            when(col("Econ_3").startswith("672"), "Social Assistance")
            .when(col("Econ_3").startswith("616"), "Pensions")
            .when(col("Econ_3").startswith("673"), "Other Social Benefits")
            .when(
                col("Econ_3").startswith("614") | col("Econ_3").startswith("615"),
                "Allowances",
            )
            .when(is_wage_bill, "Basic Wages")
            .when(
                col("Econ_3").startswith("624") | col("Econ_3").startswith("635"),
                "Basic Services",
            )
            .when(col("Econ_3").startswith("627"), "Employment Contracts")
            .when(col("Econ_3").startswith("625"), "Recurrent Maintenance")
            .when(col("Econ_1").startswith("5 "), "Subsidies to Production"),
        )
        .withColumn(
            "econ",
            # Social benefits owns the 616/672/673 intersection with Wage bill;
            # Wage bill owns the 6212 intersection with Goods and services.
            when(is_social_benefit_code, "Social benefits")
            .when(is_wage_bill, "Wage bill")
            .when(col("Econ_1").startswith("4 "), "Capital expenditures")
            .when(is_goods_and_services, "Goods and services")
            .when(col("Econ_1").startswith("5 "), "Subsidies")
            .when(
                col("Econ_3").startswith("664") | col("Econ_3").startswith("666"),
                "Other grants and transfers",
            )
            .when(col("Econ_1").startswith("3 "), "Interest on debt")
            .otherwise("Other expenses"),
        )
        # FY2015 has no direct execution column in the workbook. Preserve its
        # approved-times-execution-scale method. Every other year, including
        # FY2024, uses Ordered_to_pay directly.
        .withColumn(
            "executed",
            when(
                col("Year") == 2015,
                col("Credit") * lit(719503643911 / 793650121655),
            ).otherwise(col("Ordered_to_pay")),
        )
        # Keep func_sub consistent with the final func owner.
        .withColumn(
            "func_sub",
            when(col("func") == "Social protection", lit(None).cast("string"))
            .when(
                col("func") == "Housing and community amenities",
                when(col("_is_water_and_sanitation"), "Water Supply").otherwise(
                    lit(None).cast("string")
                ),
            )
            .when(
                col("func") == "Public order and safety",
                when(
                    col("func_sub").isin("Judiciary", "Public Safety"),
                    col("func_sub"),
                ).otherwise(lit(None).cast("string")),
            )
            .when(
                col("func") == "Economic affairs",
                when(
                    col("func_sub").isin(
                        "Agriculture",
                        "Roads",
                        "Railroads",
                        "Air Transport",
                        "Transport",
                        "Energy",
                        "Telecom",
                    ),
                    col("func_sub"),
                ).otherwise(lit(None).cast("string")),
            )
            .when(
                col("func") == "Education",
                when(
                    col("func_sub").isin(
                        "Primary Education",
                        "Secondary Education",
                        "Tertiary Education",
                        "Primary and Secondary education",
                    ),
                    col("func_sub"),
                ).otherwise(lit(None).cast("string")),
            )
            .otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "geo0",
            when(col("geo1") == "Central Scope", "Central").otherwise("Regional"),
        )
        .withColumn("is_foreign", col("Econ_2").startswith("27 "))
        .drop(
            "_is_water_and_sanitation",
            "_is_social_protection",
            "_is_housing",
            "_is_defence",
            "_is_public_order",
            "_is_economic_affairs",
            "_is_environment",
            "_is_health",
            "_is_recreation",
            "_is_education",
        )
    )
    return df

# The DLT output is line-level, so it does not need 26 separate Excel aggregate
# formulas. The same classification rules run for all rows, and this expectation
# confirms that FY2024 Executed coverage is present in the published output.
@dlt.expect("fy2024_executed_available", "year <> 2024 OR executed IS NOT NULL")
@dlt.expect("classifications_available", "func IS NOT NULL AND econ IS NOT NULL")
@dlt.table(name="bdi_boost_gold")
def boost_gold():
    return (
        dlt.read("bdi_boost_silver")
        .withColumn("country_name", lit(COUNTRY))
        .select(
            "country_name",
            col("Year").alias("year").cast("int"),
            col("Credit").alias("approved").cast(DoubleType()),
            expr("CAST(NULL AS DOUBLE) as revised"),
            col("executed").cast(DoubleType()),
            "admin0",
            "admin1",
            "admin2",
            "geo0",
            "geo1",
            "is_foreign",
            "func",
            "func_sub",
            "econ",
            "econ_sub",
        )
    )
