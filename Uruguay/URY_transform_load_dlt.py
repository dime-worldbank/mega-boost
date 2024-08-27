# Databricks notebook source
import dlt
import unicodedata
from pyspark.sql.functions import (
    substring,
    col,
    lit,
    when,
    udf,
    trim,
    regexp_replace,
    initcap,
    concat,
    lower,
)
from pyspark.sql.types import StringType

# Note DLT requires the path to not start with /dbfs
TOP_DIR = "/mnt/DAP/data/BOOSTProcessed"
INPUT_DIR = f"{TOP_DIR}/Documents/input/Countries"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = "Uruguay"
COUNTRY_MICRODATA_DIR = f"{WORKSPACE_DIR}/microdata_csv/{COUNTRY}"

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
}


@dlt.expect_or_drop("year_not_null", "YEAR IS NOT NULL")
@dlt.table(name=f"ury_boost_bronze")
def boost_bronze():
    # Load the data from CSV
    return (
        spark.read.format("csv")
        .options(**CSV_READ_OPTIONS)
        .option("inferSchema", "true")
        .load(f"{COUNTRY_MICRODATA_DIR}")
    )


@dlt.table(name=f"ury_boost_silver")
def boost_silver():
    return (
        dlt.read(f"ury_boost_bronze")
        .withColumn("econ2_lower", lower(col("econ2")))
        .withColumn("admin0", lit("Central"))  # No subnational data available
        .withColumn("geo1", lit("Central Scope"))  # No subnational data available
        .withColumn("is_foreign", col("SOURCE_FIN1").startswith("20 "))
        .withColumn(
            "func_sub",
            when(col("func1").startswith("01 "), "judiciary")
            .when(col("func1").startswith("14 "), "public safety")
            .when(col("admin1").startswith("07 "), "agriculture")
            .when(
                col("func1").startswith("09 ")
                & ~col("func2").startswith("0368 ")
                & ~col("func2").startswith("0369 "),
                "transport",
            )
            .when(
                col("func1").startswith("09 ")
                & (
                    col("func2").startswith("0362 ")
                    | col("func2").startswith("0370 ")
                    | col("func2").startswith("0371 ")
                    | col("func2").startswith("0372 ")
                ),
                "road transport",
            )
            .when(
                col("func1").startswith("09 ") & col("func2").startswith("0367 "),
                "air transport",
            )
            .when(
                col("func1").startswith("09 ") & col("func2").startswith("0369 "),
                "telecom",
            )
            .when(
                (
                    (col("year") < 2016)
                    & (
                        col("func1").startswith("09 ")
                        & col("func2").startswith("0368 ")
                    )
                )
                | ((col("year") >= 2016) & (col("func1").startswith("18 "))),
                "energy",
            )
            .when(
                (
                    (col("year") < 2016)
                    & (
                        col("func1").startswith("08 ")
                        & (
                            col("func2").startswith("0002 ")
                            | (col("func2").startswith("0345 "))
                        )
                    )
                )
                | (
                    (col("year") >= 2016)
                    & (
                        col("func1").startswith("08 ")
                        & (
                            col("func2").startswith("0002 ")
                            | (col("func2").startswith("0345 "))
                            | (col("func2").startswith("0602 "))
                            | (col("func2").startswith("0603 "))
                        )
                    )
                ),
                "primary education",
            )
            .when(
                (
                    ((col("year") < 2016) & (col("func1").startswith("08 ")))
                    & (
                        col("func2").startswith("0003 ")
                        | col("func2").startswith("0346 ")
                    )
                )
                | (
                    ((col("year") >= 2016) & (col("func1").startswith("08 ")))
                    & (
                        col("func2").startswith("0003 ")
                        | col("func2").startswith("0004 ")
                        | col("func2").startswith("0346 ")
                        | col("func2").startswith("0604 ")
                        | col("func2").startswith("0605 ")
                    )
                ),
                "secondary education",
            )
            .when(
                col("func1").startswith("08 ")
                & (
                    col("func2").startswith("0351 ")
                    | col("func2").startswith("0347 ")
                    | col("func2").startswith("0349 ")
                    | col("func2").startswith("0353 ")
                ),
                "tertiary education",
            ),
        )
        .withColumn(
            "func",
            when(col("func1").startswith("06 "), "Defence")
            .when(
                col("func_sub").isin("judiciary", "public safety"),
                "Public order and safety",
            )
            .when(
                (col("year") <= 2017)
                & (
                    col("func1").startswith("03 ")
                    | col("func1").startswith("07 ")
                    | col("func1").startswith("16 ")
                    | col("func1").startswith("09 ")
                    | col("func1").startswith("18 ")
                ),
                "Economic affairs",
            )
            .when(
                (
                    (col("year") > 2017)
                    & (
                        (col("func1").startswith("07 "))
                        | (col("func1").startswith("16 "))
                        | (col("func1").startswith("09 "))
                        | (col("func1").startswith("18 "))
                    )
                ),
                "Economic affairs",
            )
            .when(col("func1").startswith("10 "), "Environmental protection")
            .when(col("func1").startswith("17 "), "Housing and community amenities")
            .when(col("func1").startswith("13 "), "Health")
            .when(col("func1").startswith("05 "), "Recreation, culture and religion")
            .when(col("func1").startswith("08 "), "Education")
            .when(
                ((col("year") < 2020) & (col("func1").startswith("11 ")))
                | (
                    (col("year") >= 2020)
                    & (
                        (col("func1").startswith("19 "))
                        | (col("func1").startswith("20 "))
                    )
                ),
                "Social protection",
            )
            .otherwise("General public services"),
        )
        .withColumn(
            "econ_sub",
            when(
                (col("exp_type") == "Personal")
                & (col("econ2_lower") != "06 beneficios al personal")
                & (col("econ2_lower") != "07 beneficios familiares")
                & (
                    col("econ2_lower") != "08 cargas legales sobre servicios personales"
                ),
                "basic wages",
            )
            .when(col("exp_type") == "Personal", "allowances")
            .when(
                (lower(col("exp_type")) == "inversion")
                & col("source_fin1").startswith("20 "),
                "capital expenditure (foreign spending)",
            )
            .when(col("econ2_lower") == "21 servicios basicos", "basic services")
            .when(
                (
                    (col("year") < 2019)
                    & (
                        col("econ2_lower")
                        == "28 servicios tecnicos, profesionales y artisticos(dec.17/003)"
                    )
                )
                | (
                    (col("year") >= 2019)
                    & (col("econ2_lower").startswith("08 servicios tecnicos"))
                ),
                "employment contracts",
            )
            .when(
                col("econ2_lower")
                == "07 servici. para mantenimiento, reparaciones menores y limpieza",
                "recurrent maintenance",
            )
            .when(
                (
                    (col("year") < 2020)
                    & (
                        col("econ2_lower")
                        == "52 transferencias corrientes al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "54 transferencias de capital al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "04 transferencias de capital al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "02 transferencias corrientes al sector privado"
                    )
                ),
                "subsidies to production",
            )
            .when(
                (col("year") >= 2020)
                & (
                    (
                        col("econ2_lower")
                        == "04 transferencias de capital al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "02 transferencias corrientes al sector privado"
                    )
                ),
                "subsidies to production",
            )
            .when(
                (
                    (col("year") <= 2019)
                    & (
                        col("func1").startswith("11 ")
                        & col("func2").startswith("0402")
                        & col("econ1").startswith("5 ")
                    )
                )
                | (
                    (col("year") > 2019)
                    & (col("func1").startswith("20 ") & col("func1").startswith("5 "))
                ),
                "pensions",
            )
            .when(
                col("func1").startswith("11 ") & col("econ1").startswith("5 "),
                "social assistance",
            ),
        )
        .withColumn(
            "econ",
            when(col("econ1").startswith("6 "), "Interest on debt")
            .when(col("exp_type") == "Personal", "Wage bill")
            .when(
                (
                    (lower(col("exp_type")) == "inversion")
                    & (
                        col("econ2_lower")
                        != "02 transferencias corrientes al sector privado"
                    )
                    & (
                        col("econ2_lower")
                        != "04 transferencias de capital al sector privado"
                    )
                ),
                "Capital expenditures",
            )
            .when(
                col("econ1").startswith("1 ")
                | col("econ1").startswith("2 ")
                | ((col("econ1").startswith("3 ")) & (col("exp_type") != "Inversion")),
                "Goods and services",
            )
            .when(
                (
                    (col("year") < 2020)
                    & (
                        col("econ2_lower")
                        == "52 transferencias corrientes al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "54 transferencias de capital al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "04 transferencias de capital al sector privado"
                    )
                    | (
                        col("econ2_lower")
                        == "02 transferencias corrientes al sector privado"
                    )
                ),
                "Subsidies",
            )
            .when(
                (
                    (col("year") >= 2020)
                    & (
                        (
                            col("econ2_lower")
                            == "04 transferencias de capital al sector privado"
                        )
                        | (
                            col("econ2_lower")
                            == "02 transferencias corrientes al sector privado"
                        )
                    )
                ),
                "Subsidies",
            )
            .when(
                col("econ_sub").isin("pensions", "social assistance"), "Social benefits"
            )
            .when(
                (
                    (col("year") < 2019)
                    & (
                        (
                            (
                                col("econ2_lower")
                                == "01 transferencias corrientes al sector publico"
                            )
                            | (
                                col("econ2_lower")
                                == "51 transferencias corrientes al sector publico"
                            )
                        )
                        & (
                            (~col("func1").startswith("11 "))
                            & (col("exp_type") != "Inversion")
                        )
                    )
                )
                | (
                    (col("year") >= 2019)
                    & (
                        (
                            (
                                col("econ2_lower")
                                == "01 transferencias corrientes al sector publico"
                            )
                            | (
                                col("econ2_lower")
                                == "51 transferencias corrientes al sector publico"
                            )
                        )
                        & (
                            (~col("func1").startswith("11 "))
                            & (~col("func1").startswith("19 "))
                            & (~col("func1").startswith("20 "))
                            & (col("exp_type") != "Inversion")
                        )
                    )
                ),
                "Other grants and transfers",
            )
            .otherwise("Other expenses"),
        )
    )


@dlt.table(name=f"ury_boost_gold")
def boost_gold():
    return (
        dlt.read(f"ury_boost_silver")
        .withColumn("country_name", lit(COUNTRY))
        .withColumn("admin2", col("admin1"))
        .withColumn("admin1", lit("Central Scope"))
        .select(
            "country_name",
            "year",
            "approved",
            "executed",
            "is_foreign",
            "geo1",
            "admin0",
            "admin1",
            "admin2",
            "func_sub",
            "func",
            "econ_sub",
            "econ",
        )
    )