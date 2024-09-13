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

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
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
    for old_col_name in bronze_df.columns:
        new_col_name = old_col_name.replace(" ", "_")
        bronze_df = bronze_df.withColumnRenamed(old_col_name, new_col_name)
    return bronze_df


@dlt.table(name=f"zaf_boost_silver")
def boost_silver():
    return (
        (
            dlt.read(f"zaf_boost_bronze")
            .withColumn("econ1", coalesce(col("Economic_Level_1"), lit("")))
            .withColumn("econ2", coalesce(col("Economic_Level_2"), lit("")))
            .withColumn("econ3", coalesce(col("Economic_Level_3"), lit("")))
            .withColumn("func_lower", coalesce(lower(col("Function_group")), lit("")))
            .withColumn("func_sub_lower", coalesce(lower(col("Budget_group")), lit("")))
            .withColumn("admin0", coalesce(col("Admin1"), lit("")))
            .withColumn("admin0_temp", coalesce(col("Admin1"), lit("")))
            .withColumn("admin1", coalesce(col("Department"), lit("")))
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
                (
                    (col("admin0_temp") == "Municipalities")
                    | (col("admin0_temp") == "Provinces")
                ),
                "Regional",
            ).when((col("admin0_temp") == "Central"), "Central"),
        )
        .withColumn(
            "admin1",
            when(col("admin0") == "Central", "Central Scope").when(
                col("admin0") == "Regional", col("admin1")
            ),
        )
        .withColumn(
            "geo1",
            when(col("admin0").isNull(), "Central Scope")
            .when(
                col("admin0") == "Central",
                "Central Scope",
            )
            .otherwise(
                col("admin1"),
            ),
        )
        .withColumn(
            "func_sub",
            when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                )
                & (
                    (col("program") == "02 superior court services")
                    | (col("program") == "02 court services")
                ),
                "judiciary",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                )
                & (
                    (col("program") == "02 visible policing")
                    | (col("program") == "02 incarceration")
                ),
                "public safety",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                )
                & (
                    (col("func_sub_lower") == "agriculture and rural development")
                    | (col("func_sub_lower") == "agriculture")
                ),
                "agriculture",
            )
            .when(
                ((col("econ1") != "Payments for financial assets"))
                & (
                    (
                        col("program").isin(
                            "04 road transport",
                            "2. transport infrastructure",
                            "roads infrastructure",
                        )
                    )
                    | (
                        col("func_sub_lower").isin(
                            "public works, roads and transport", "road transport"
                        )
                    )
                    | (col("transfers").startswith("south african national roads"))
                ),
                "roads",
            )
            .when(
                ((col("econ1") != "Payments for financial assets"))
                & (
                    (col("func_lower") == "03 rail Transport")
                    | (col("func_lower") == "05 gautrain")
                ),
                "railroads",
            )
            .when(
                ((col("econ1") != "Payments for financial assets"))
                & ((col("program") == "05 civil aviation")),
                "air transport",
            )
            .when(
                (col("econ1") != "Payments for financial assets")
                & (
                    (
                        (col("Admin0_temp") == "Municipalities")
                        & (col("func_sub_lower") == "water")
                    )
                    | (
                        (col("Admin0_temp") == "Central")
                        & (col("program").startswith("03 water"))
                    )
                ),
                "water and sanitation",
            )
            .when(
                (col("econ1") != "Payments for financial assets")
                & (col("econ2") != "Provinces and municipalities")
                & (col("program") == "03 university education"),
                "tertiary education",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (
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
                "energy",
            ),
        )
        .withColumn(
            "func",
            when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                    & (
                        col("program").isin(
                            "03 landward defence",
                            "04 air defence",
                            "05 maritime defence",
                            "07 defence intelligence",
                        )
                    )
                ),
                "Defence",
            )
            # public order and safety
            .when(
                col("func_sub_lower").isin("public safety", "judiciary"),
                "Public order and safety",
            ).when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("func_lower").startswith("economic"))
                ),
                "Economic affairs",
            )
            # environment protection
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (
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
                            & (~col("admin1").startswith("32"))
                        )
                        | (col("admin1").startswith("32"))
                    )
                ),
                "Environmental protection",
            )
            .when(
                col("func_sub") == "water and sanitation",
                "Housing and community amenities",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                    & (
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
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                    & (
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
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                    & (
                        col("func_sub_lower").isin(
                            "social development",
                            "Social protection",
                        )
                    )
                ),
                "Social Protection",
            )
            # general public services
            .otherwise("General public services"),
        )
        .withColumn(
            "econ_sub",
            when(
                (col("econ1") != "Payments for financial assets")
                & (col("econ3") != "Social contributions"),
                "social benefits (pension contributions)",
            )
            .when(
                (col("econ1") != "Payments for financial assets")
                & (col("econ3").isin("Property payments", "Operating leases")),
                "basic services",
            )
            .when(
                (col("econ1") != "Payments for financial assets")
                & (col("econ3") == "Contractors"),
                "employment contracts",
            )
            .when(
                (col("econ1") != "Payments for financial assets")
                & (col("econ2") == "Public corporations and private enterprises"),
                "subsidies to production",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                    & (
                        col("func_sub_lower").isin(
                            "social development",
                            "social protection",
                        )
                    )
                ),
                "social assistance",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") != "Provinces and municipalities")
                    & (col("transfers") == "old age")
                ),
                "pensions",
            ),
        )
        .withColumn(
            "econ",
            # wage bill
            when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") == "Compensation of employees")
                ),
                "wage bill",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ1") == "Payments for capital assets")
                ),
                "capital expenditures",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (col("econ2") == "Goods and services")
                ),
                "Goods and services",
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (
                        col("econ2").isin(
                            "Non-profit institutions",
                            "Public corporations and private enterprises",
                        )
                    )
                ),
                "Subsidies",
            )
            # social benefits
            .when(
                col("econ_sub").isin("social assistance", "pensions"), "Social benefits"
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (
                        col("econ2").isin(
                            "Departmental agencies and accounts",
                            "Higher education institutions",
                        )
                    )
                ),
                "Other grants and transfers",
            )
            .when(
                col("econ_sub").isin("social assistance", "pensions"), "Social benefits"
            )
            .when(
                (
                    (col("econ1") != "Payments for financial assets")
                    & (
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
        .filter(col("econ1") != "Payments for financial assets")  # debt repayment
        .withColumn("country_name", lit("South Africa"))
        .select(
            "country_name",
            "year",
            col("budget").alias("approved"),
            col("actuals").alias("executed"),
            "admin0",
            "admin1",
            "geo1",
            "func",
            "func_sub",
            "econ",
            "econ_sub",
        )
    )

# COMMAND ----------

