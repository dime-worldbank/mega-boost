# Databricks notebook source
import dlt
from pyspark.sql.functions import col, lit, when

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Togo'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}
@dlt.expect_or_drop("year_not_null", "year IS NOT NULL")
@dlt.table(name="tgo_2021_onward_boost_bronze")
def bronze_table():
    return (
        spark.read
        .format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(COUNTRY_MICRODATA_DIR)
    ).withColumn(
        "year", col("YEAR").cast("int")
    )

# COMMAND ----------

@dlt.table(name="tgo_2021_onward_boost_silver")
def silver_table():
    return (
        dlt.read("tgo_2021_onward_boost_bronze")
        # Foreign funding flag based on SOURCE_FIN1
        .withColumn("is_foreign", ~col("ADMIN5").like("FINANCEMENTS INTERNES%"))

        # Administrative levels
        .withColumn("admin0", lit("Central"))
        .withColumn("admin1", lit("Central Scope"))

        # Geographic levels
        .withColumn(
            "geo0",
            when(
                (col("REGION").isNotNull()) & (col("REGION") != "AUTRES REGIONS"),
                "Regional",
            ).otherwise("Central"),
        )
        .withColumn(
            "geo1",
            when(col("geo0") == "Cenral", "Central Scope").otherwise(col("REGION")),
        )
        .withColumn("geo2", col("PREFECTURE"))

        # Functional classifications
        .withColumn(
            "func",
            when(
                col("CODE_FUNC1") == "01",
                "General public services",
            )
            .when(col("CODE_FUNC1") == "02", "Defence")
            .when(
                col("CODE_FUNC1") == "03",
                "Public order and safety",
            )
            .when(col("CODE_FUNC1") == "04", "Economic affairs")
            .when(
                col("CODE_FUNC1") == "05",
                "Environmental protection",
            )
            .when(
                col("CODE_FUNC1") == "06",
                "Housing and community amenities",
            )
            .when(col("CODE_FUNC1") == "07", "Health")
            .when(
                col("CODE_FUNC1") == "08",
                "Recreation, culture and religion",
            )
            .when(col("CODE_FUNC1") == "09", "Education")
            .when(col("CODE_FUNC1") == "10", "Social protection"),
        )
        .withColumn(
            "func_sub",
            when(
                col("CODE_FUNC1") == "03",
                when(col("CODE_FUNC2") == "033", "Judiciary")
                .otherwise("Public Safety"),
            ).when(
                col("CODE_FUNC1") == "09",
                when(col("CODE_FUNC2") == "091", "Primary Education")
                .when(col("CODE_FUNC2") == "092", "Secondary Education")
                .when(col("CODE_FUNC2") == "094", "Tertiary Education")
            ) # TODO: "07" "Health", "04" "Economic affairs" 
              # Reference Togo BOOST CCI Executed sheet for mapping & see standardized econ_sub values: https://github.com/dime-worldbank/mega-boost/blob/main/quality/transform_load_dlt.py#L134-L149 
        )

        # Economic classifications
        .withColumn(
            "econ",
            when(col("CODE_ECON1") == "1", "Interest on debt")
            .when(col("CODE_ECON1") == "2", "Wage bill")
            .when(col("CODE_ECON1") == "3", "Goods and services")
            .when(col("CODE_ECON1") == "5", "Capital expenditures")
            # TODO: "Subsidies", "Social benefits", "Other grants and transfers", See how it's done for past years in Togo BOOST CCI Executed sheet
            .otherwise("Other expenses")
        )
        .withColumn(
            "econ_sub", 
            when(
                col("CODE_ECON1") == "2",
                when(col("CODE_FUNC2").isin("661", "665"), "Basic Wages")
                .when(col("CODE_FUNC2") == "663", "Allowances")
                .when(col("CODE_FUNC2").isin("664", "666"), "Social Benefits (pension contributions)")
            ).when(
                col("CODE_ECON1") == "3",
                when(col("CODE_ECON3") == "614", "Recurrent Maintenance")
                # TODO: "Basic Services", "Employment Contracts"
            ).when(
                col("CODE_ECON1") == "5",
                when(col("is_foreign"), "Capital Expenditure (foreign spending)")
                # TODO: "Capital Maintenance"
            )
            # TODO: within Subsideies: "Subsidies to Production"
            # TODO: within Social benefits: "Social Assistance", "Pensions", "Other Social Benefits"
        )
    )

# COMMAND ----------

@dlt.table(name=f'tgo_boost_gold')
def boost_gold():
    gold_cols = [
        'year',
        'admin0',
        'admin1',
        col('ADMIN2').alias('admin2'),
        'geo0',
        'geo1',
        'func',
        'func_sub',
        'econ',
        'econ_sub',
        'is_foreign',
        col("ORDONNANCER").alias("executed"),
        col("DOTATION_INITIALE").alias("approved"),
        col("DOTATION_FINALE").alias("revised"),
    ]

    return (
        dlt.read(f'tgo_2021_onward_boost_silver')
        .select(*gold_cols)
    )
