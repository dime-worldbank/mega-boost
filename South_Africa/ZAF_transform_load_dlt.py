# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    substring,
    col,
    lit,
    when,
    element_at,
    split,
    upper,
    trim,
    lower,
    regexp_replace,
    regexp_extract,
    substring,
    coalesce,
)
from pyspark.sql.types import DoubleType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY_MICRODATA_DIR = f"{WORKSPACE_DIR}/microdata_csv/South Africa"

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f"zaf_boost_bronze")
def boost_bronze():
    bronze_df = (
        spark.read.format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(COUNTRY_MICRODATA_DIR)
    )
    return bronze_df


@dlt.table(name=f"zaf_boost_silver")
def boost_silver():
    return (
        (
            dlt.read(f"zaf_boost_bronze")
            .filter(
                (col("Economic_Level_1") != "Payments for financial assets")
                & (col("Economic_Level_2") != "Provinces and municipalities")
            )
            .withColumn("econ1", coalesce(col("Economic_Level_1"), lit("")))
            .withColumn("econ2", coalesce(col("Economic_Level_2"), lit("")))
            .withColumn("econ3", coalesce(col("Economic_Level_3"), lit("")))
            .withColumn("func_lower", coalesce(lower(col("Function_group")), lit("")))
            .withColumn("func_sub_lower", coalesce(lower(col("Budget_group")), lit("")))
            .withColumn("admin0_temp", coalesce(col("Admin1"), lit("")))
            .withColumn("Department", coalesce(col("Department"), lit("")))
            .withColumn("year", col("Year").cast("int"))
            .withColumn("program", lower(col("Programme")))
            .withColumn(
                "transfers",
                lower(
                    col(
                        "Transfers_and_subsidies_/_Payments_for_financial_assets_detail"
                    )
                ),
            )
        )
        .withColumn(
            "admin0",
            when(
                col("admin0_temp").isin("Provinces", "Municipalities"),
                "Regional",
            ).otherwise("Central"),
        )
        .withColumn(
            "admin1",
            when(col("admin0") == "Regional", col("Department"),).otherwise(
                "Central Scope",
            ),
        )
        .withColumn(
            "func_sub",
            when(
                (
                    (col("program") == "02 superior court services")
                    | (col("program") == "02 court services")
                ),
                "Judiciary",
            )
            .when(
                (
                    (col("program") == "02 visible policing")
                    | (col("program") == "02 incarceration")
                ),
                "Public Safety",
            )
            .when(
                (
                    (col("func_sub_lower") == "agriculture and rural development")
                    | (col("func_sub_lower") == "agriculture")
                ),
                "Agriculture",
            )
            .when(
                (
                    col("program").isin(
                        "04 road transport",
                        "2. transport infrastructure",
                        "roads infrastructure",
                    )
                )
                | (
                    col("func_sub_lower").isin(
                        "public works, roads and transport", "roads"
                    )
                )
                | (col("transfers").startswith("south african national roads")),
                "Roads",
            )
            .when(
                (
                    (col("func_lower") == "03 rail Transport")
                    | (col("func_lower") == "05 gautrain")
                ),
                "Water Transport",
            )
            .when(
                (col("program") == "05 civil aviation"),
                "Air Transport",
            )
            .when(
                (
                    (
                        (col("Admin0_temp") == "Municipalities")
                        & (col("func_sub_lower") == "water")
                    )
                    | (
                        (col("Admin0_temp") == "Central")
                        & (col("program").startswith("03 water"))
                    )
                ),
                "Water and Sanitation",
            )
            .when(
                (col("program") == "03 university education"),
                "Tertiary Education",
            )
            .when(
                (
                    (
                        (
                            (col("Admin0_temp") == "Central")
                            & (
                                col("program").isin(
                                    "02 energy policy and planning",
                                    "04 electrification and energy programme and project management",
                                    "05 nuclear energy",
                                    "06 clean energy",
                                    "03 mining, minerals and energy policy development",
                                    "05 mineral and energy resources programmes and projects",
                                )
                            )
                        )
                        | (
                            (col("Admin0_temp") == "Municipalities")
                            & (col("func_sub_lower") == "electricity")
                        )
                    )
                ),
                "Energy",
            ),
        )
        .withColumn(
            "func",
            when(
                (
                    col("program").isin(
                        "03 landward defence",
                        "04 air defence",
                        "05 maritime defence",
                        "07 defence intelligence",
                    )
                ),
                "Defence",
            )
            # public order and safety
            .when(
                col("func_sub").isin("Public Safety", "Judiciary"),
                "Public order and safety",
            ).when(
                ((col("func_lower").startswith("economic"))),
                "Economic affairs",
            )
            # environment protection
            .when(
                (
                    (
                        (
                            (col("program").startswith("04 climate"))
                            | (col("program").startswith("02 enviro"))
                            | (col("program").startswith("03 enviro"))
                            | (col("program").startswith("04 enviro"))
                            | (col("program").startswith("06 enviro"))
                            | (col("program").startswith("07 enviro"))
                            | (col("program").startswith("08 enviro"))
                            | (col("program").startswith("09 enviro"))
                            | (
                                col("program").isin(
                                    "05 forestry and natural resources management",
                                    "07 chemicals and waste management",
                                )
                            )
                        )
                        & (~col("Department").startswith("32"))
                    )
                    | (col("Department").startswith("32"))
                ),
                "Environmental protection",
            )
            .when(
                col("func_sub") == "Water and Sanitation",
                "Housing and community amenities",
            )
            .when(
                (
                    (
                        col("func_sub_lower").isin(
                            "health",
                            "clinics",
                        )
                    )
                ),
                "Health",
            )
            .when(
                (
                    (
                        col("func_sub_lower").isin(
                            "basic education",
                            "education",
                        )
                    )
                ),
                "Education",
            )
            .when(
                (
                    (
                        col("func_sub_lower").isin(
                            "social development",
                            "social protection",
                        )
                    )
                ),
                "Social protection",
            )
            # general public services
            .otherwise("General public services"),
        )
        .withColumn(
            "econ_sub",
            when(
                (col("econ3") == "Social contributions"),
                "Social Benefits (pension contributions)",
            )
            .when(
                col("econ3").isin("Property payments", "Operating leases"),
                "Basic Services",
            )
            .when(
                (col("econ3") == "Contractors"),
                "Employment Contracts",
            )
            .when(
                (col("econ2") == "Public corporations and private enterprises"),
                "Subsidies to Production",
            )
            .when(
                (col("transfers") == "old age"),
                "Pensions",
            )
            .when(
                (
                    (col("econ3") != "Property payments")
                    & (col("econ3") != "Operating leases")
                    & (col("econ3") != "Social contributions")
                    & (col("econ3") != "Contractors")
                    & (col("econ2") != "Public corporations and private enterprises")
                    & (col("econ2") != "Non-profit institutions")
                    & (col("econ2") != "Compensation of employees")
                    & (col("econ2") != "Goods and services")
                    & (col("econ1") != "Payments for capital assets")
                    & (col("econ2") != "Departmental agencies and accounts")
                    & (col("econ2") != "Higher education institutions")
                    & (
                        (col("func_sub_lower") == "social protection")
                        | ((col("func_sub_lower") == "social development"))
                    )
                ),
                "Social Assistance",
            ),
        )
        .withColumn(
            "econ",
            # wage bill
            when(
                ((col("econ2") == "Compensation of employees")),
                "Wage bill",
            )
            .when(
                ((col("econ1") == "Payments for capital assets")),
                "Capital expenditures",
            )
            .when(
                ((col("econ2") == "Goods and services")),
                "Goods and services",
            )
            .when(
                (
                    (
                        col("econ2").isin(
                            "Non-profit institutions",
                            "Public corporations and private enterprises",
                        )
                    )
                ),
                "Subsidies",
            )
            .when(
                (
                    (
                        col("econ2").isin(
                            "Departmental agencies and accounts",
                            "Higher education institutions",
                        )
                    )
                ),
                "Other grants and transfers",
            )
            .when(
                col("econ_sub").isin("Social Assistance", "Pensions"), "Social benefits"
            )
            .when(
                (
                    (
                        col("econ3")
                        == "Interest (Incl. interest on unitary payments (PPP))"
                    )
                ),
                "Interest on debt",
            )
            # other expenses
            .otherwise("Other expenses"),
        )
    )


@dlt.table(name=f"zaf_boost_gold")
def boost_gold():
    return (
        dlt.read(f"zaf_boost_silver")
        .withColumn("country_name", lit("South Africa"))
        .select(
            "country_name",
            "year",
            col("budget").alias("approved").cast(DoubleType()),
            col("actuals").alias("executed").cast(DoubleType()),
            "admin0",
            "admin1",
            col("admin1").alias("geo1"),
            "func",
            "func_sub",
            "econ",
            "econ_sub",
        )
    )
