# Databricks notebook source
# Moldova BOOST DLT pipeline.
#
# Moldova's workbook splits raw microdata across three year-range sheets
# (`2006-15`, `2016-19`, `2020-24`) with different column schemas and
# different named-range suffixes in the Approved/Executed formulas. Each
# tag rule in `tag_rules.csv` carries a `measure` field that dispatches the
# rule to the correct raw sheet:
#
#   measure ∈ {approved, executed}            → 2006-15 bronze
#   measure ∈ {approved_16, executed_16}      → 2016-19 bronze
#   measure ∈ {approved_20, executed_20}      → 2020-24 bronze
#
# Revenue rules live in the same raw sheets; the 2016+ variants add an
# `econ0_* = "Revenues"` criterion to distinguish revenue rows (pre-2016
# has no econ0, so REV_* codes have no base-range variant — see
# reports/workbook_inventory.md).
#
# Two driver tables sit next to this notebook:
#   _analysis/data/tag_rules.csv       — (code, criteria_json, measure,
#                                         year_min/max, n_sumifs)
#   _analysis/data/code_dictionary.csv — (code, econ, econ_sub, func, func_sub)
#                                         EXP_CROSS_* rows excluded to avoid
#                                         double-counting against the
#                                         EXP_ECON_* × EXP_FUNC_* parents.
#
# Translation principle: apply each rule literally — no formula
# reinterpretation, no value cleaning. Anything that smells like
# interpretation gets reported in Moldova/_analysis/reports/ISSUES.md.
#
# Known limitations (flagged in ISSUES.md for SME triage):
#   - Rules with n_sumifs > 1 (Raw2 supplements, additive education
#     formulas): only the first SUMIFS block is applied here.
#   - admin0/geo0 derivation from Moldova's admin1 values ("Central"/"Local"
#     pre-2016, "centrala"/"locale" 2016+) awaits SME confirmation.
#   - `EXP_FUNC_REV_CUS_EXC_EXE`: code name is misleading; category says
#     COFOG 708 "Recreation, culture and religion" — mapped accordingly but
#     rename upstream.

import json
from pathlib import Path

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, sum as _sum, concat, monotonically_increasing_id,
)

from quality.discrepancy_review import build_review

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = "Moldova"
COUNTRY_MICRODATA_DIR = f"{WORKSPACE_DIR}/microdata_csv/{COUNTRY}"

ANALYSIS_DIR = (Path(__file__).resolve().parent / "_analysis" / "data") \
    if "__file__" in globals() else Path("Moldova/_analysis/data")

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
    "inferSchema": "true",
}

# Named-range → raw-column mapping, one per year-range sheet (keys appear in
# SUMIFS criteria; values are the CSV column headers). See
# Moldova/_analysis/data/defined_names.csv.
MAP_BASE = {  # 2006-15
    "year": "year", "admin1": "admin1",
    "func1": "func1", "func2": "func2",
    "econ1": "econ1", "econ2": "econ2",
    "exp_type": "exp_type", "transfer": "transfer",
    "approved": "approved", "executed": "executed", "adjusted": "adjusted",
}

MAP_16 = {  # 2016-19
    "year_16": "year",
    "admin1_16": "admin1", "admin2_16": "admin2",
    "func1_16": "func1", "func2_16": "func2", "func3_16": "func3",
    "econ0_16": "econ0",
    "econ1_16": "econ1", "econ2_16": "econ2", "econ3_16": "econ3",
    "econ4_16": "econ4", "econ5_16": "econ5", "econ6_16": "econ6",
    "exp_type_16": "exp_type", "transfer_16": "transfer",
    "fin_source1_16": "fin_source1",
    "approved_16": "approved", "revised_16": "revised",
    "executed_16": "executed",
    "program1_16": "program1", "program2_16": "program2",
    "activity_16": "activity",
}

MAP_20 = {  # 2020-24
    "year_20": "year",
    "admin1_20": "admin1", "admin2_20": "admin2",
    "func1_20": "func1", "func2_20": "func2", "func3_20": "func3",
    "econ0_20": "econ0",
    "econ1_20": "econ1", "econ2_20": "econ2", "econ3_20": "econ3",
    "econ4_20": "econ4", "econ5_20": "econ5", "econ6_20": "econ6",
    "exp_type_20": "exp_type", "transfer_20": "transfer",
    "fin_source1_20": "fin_source1",
    "approved_20": "approved", "adjusted_20": "adjusted",
    "executed_20": "executed",
}

RANGE_CONFIG = {
    "base": {"bronze": "mda_raw_2006_15_bronze", "mapping": MAP_BASE},
    "16":   {"bronze": "mda_raw_2016_19_bronze", "mapping": MAP_16},
    "20":   {"bronze": "mda_raw_2020_24_bronze", "mapping": MAP_20},
}


def classify_measure(measure: str) -> str | None:
    """Return the range-key a rule's measure dispatches to, or None for
    unsupported measures (e.g. Raw2 supplements)."""
    if measure in ("approved", "executed"):
        return "base"
    if measure.endswith("_16"):
        return "16"
    if measure.endswith("_20"):
        return "20"
    return None


# ---------- Bronze ----------

@dlt.table(name="mda_raw_2006_15_bronze",
           comment="Raw 2006-15 sheet from the Moldova BOOST workbook. No cleaning.")
def raw_2006_15_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/2006-15.csv"))
    return df.withColumn("id", concat(lit("mda_base_"), monotonically_increasing_id()))


@dlt.table(name="mda_raw_2016_19_bronze",
           comment="Raw 2016-19 sheet from the Moldova BOOST workbook. No cleaning.")
def raw_2016_19_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/2016-19.csv"))
    return df.withColumn("id", concat(lit("mda_16_"), monotonically_increasing_id()))


@dlt.table(name="mda_raw_2020_24_bronze",
           comment="Raw 2020-24 sheet from the Moldova BOOST workbook. No cleaning.")
def raw_2020_24_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/2020-24.csv"))
    return df.withColumn("id", concat(lit("mda_20_"), monotonically_increasing_id()))


# ---------- Tag rule application ----------

def _criterion_to_sql(field_col: str, op: str, value: str) -> str:
    """Translate a single SUMIFS criterion into a Spark SQL predicate. Excel
    wildcards (trailing `*`) map to SQL LIKE; numeric values stay unquoted
    against a cast string so int/float raw columns compare cleanly."""
    val_str = value.replace("'", "''")
    if "*" in val_str:
        like = val_str.replace("*", "%")
        cmp = "NOT LIKE" if op == "<>" else "LIKE"
        return f"lower({field_col}) {cmp} lower('{like}')"
    if op in ("=", "<>"):
        sql_op = "!=" if op == "<>" else "="
        try:
            float(val_str)
            return f"cast({field_col} as string) {sql_op} '{val_str}'"
        except ValueError:
            return f"lower(cast({field_col} as string)) {sql_op} lower('{val_str}')"
    return f"cast({field_col} as double) {op} {val_str}"


def _apply_rule(df: DataFrame, criteria: list[dict],
                named_range_to_col: dict[str, str]) -> DataFrame:
    where_parts = []
    for c in criteria:
        if c["op"] == "=year":
            continue  # year handled via groupBy(year) downstream
        col_name = named_range_to_col.get(c["field"])
        if col_name is None:
            # Unknown named range (e.g. a Raw2-sheet direct reference) —
            # flagged in ISSUES.md; the rule's primary SUMIFS still applies,
            # just without this criterion.
            continue
        where_parts.append(_criterion_to_sql(col_name, c["op"], c["value"]))
    if where_parts:
        df = df.where(" AND ".join(where_parts))
    return df


def _load_driver_csv(name: str) -> list[dict]:
    import csv as _csv
    p = ANALYSIS_DIR / name
    with open(p) as f:
        return list(_csv.DictReader(f))


def _silver_for_kind(kind_filter) -> DataFrame:
    """Build a (year, code, approved, executed) silver DataFrame. For each
    matching TAG rule, dispatch to the bronze corresponding to its measure
    and sum approved/executed grouped by year. Results from the three
    year-ranges union into one DataFrame. `kind_filter(rule)` decides which
    rules participate (EXP vs REV)."""
    rules = _load_driver_csv("tag_rules.csv")
    code_dict = {r["code"]: r for r in _load_driver_csv("code_dictionary.csv")}

    relevant = [
        r for r in rules
        if r["row_type"] == "TAG"
        and r["sheet"] == "Approved"
        and r["code"] in code_dict
        and classify_measure(r["measure"]) is not None
        and kind_filter(r)
    ]

    bronzes = {rk: dlt.read(cfg["bronze"]) for rk, cfg in RANGE_CONFIG.items()}

    per_rule_dfs = []
    for r in relevant:
        rk = classify_measure(r["measure"])
        criteria = json.loads(r["criteria_json"])
        filtered = _apply_rule(bronzes[rk], criteria, RANGE_CONFIG[rk]["mapping"])
        agg = (filtered
               .groupBy(col("year").cast("int").alias("year"))
               .agg(_sum("approved").alias("approved"),
                    _sum("executed").alias("executed"))
               .withColumn("code", lit(r["code"])))
        per_rule_dfs.append(agg)

    if not per_rule_dfs:
        any_bronze = next(iter(bronzes.values()))
        return any_bronze.limit(0).select(
            lit(None).cast("int").alias("year"),
            lit(None).cast("string").alias("code"),
            lit(None).cast("double").alias("approved"),
            lit(None).cast("double").alias("executed"),
        )
    silver = per_rule_dfs[0]
    for d in per_rule_dfs[1:]:
        silver = silver.unionByName(d)
    return silver.select("year", "code", "approved", "executed")


@dlt.table(name="mda_expenditure_silver",
           comment="One row per (year, code) — applies EXP_* TAG rules to the "
                   "matching year-range bronze.")
def expenditure_silver():
    return _silver_for_kind(lambda r: r["code"].startswith("EXP_"))


@dlt.table(name="mda_revenue_silver",
           comment="One row per (year, code) — applies REV_* TAG rules to the "
                   "matching year-range bronze. (Pre-2016 has no revenue data "
                   "in the workbook, so no base-range REV rules exist.)")
def revenue_silver():
    return _silver_for_kind(lambda r: r["code"].startswith("REV_"))


# ---------- Gold ----------

@dlt.table(name="mda_boost_gold",
           comment="Moldova BOOST expenditure gold. Schema matches "
                   "cross_country_aggregate_dlt.boost_gold.")
def boost_gold():
    silver = dlt.read("mda_expenditure_silver")
    code_dict_rows = _load_driver_csv("code_dictionary.csv")
    code_df = spark.createDataFrame([
        {"code": r["code"],
         "econ": r["econ"] or None,
         "econ_sub": r["econ_sub"] or None,
         "func": r["func"] or None,
         "func_sub": r["func_sub"] or None}
        for r in code_dict_rows
    ])

    return (silver.join(code_df, on="code", how="left")
            .withColumn("country_name", lit("Moldova"))
            # admin0/geo0/admin1/admin2 not carried through silver today.
            # Moldova's admin1 values differ by era ("Central"/"Local" pre-2016,
            # "centrala"/"locale" 2016+) — SME confirmation pending before
            # deriving admin0 from the raw admin1 column. See ISSUES.md.
            .withColumn("admin0", lit("Central"))
            .withColumn("admin1", lit(None).cast("string"))
            .withColumn("admin2", lit(None).cast("string"))
            .withColumn("geo0",   lit("Central"))
            .withColumn("geo1",   lit(None).cast("string"))
            .withColumn("revised", lit(None).cast("double"))
            .select("country_name", "year",
                    "admin0", "admin1", "admin2", "geo0", "geo1",
                    "func", "func_sub", "econ", "econ_sub",
                    "approved", "revised", "executed"))


# COMMAND ----------
# Phase 8 — regenerate the SME discrepancy review doc.
# Compares pipeline silver against the Excel Executed sheet at (year, code)
# granularity; preserves prior expert resolutions across runs.

from utils import input_excel_filename  # noqa: E402

review_path = build_review(
    country_code="mda",
    excel_path=input_excel_filename(COUNTRY),
    pipeline_df=spark.table("mda_expenditure_silver"),
    dimension_cols=["year", "code"],
    value_cols=["approved", "executed"],
    excel_sheet="Executed",
    threshold=0.05,
    pipeline_source_ref="Moldova/MDA_transform_load_raw_dlt.py:mda_expenditure_silver",
)
print(f"Discrepancy review written → {review_path}")
