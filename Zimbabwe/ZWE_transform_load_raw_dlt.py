# Databricks notebook source
# Zimbabwe BOOST DLT pipeline.
#
# Translation principle: the Approved/Executed sheets in the Zimbabwe workbook
# encode the BOOST classification taxonomy as 66 SUMIFS rules (see
# Zimbabwe/_analysis/tag_rules.csv). Each rule's criteria (named ranges →
# Expenditure / Revenue columns) IS the classification logic. The pipeline
# applies each rule literally — no formula reinterpretation, no value cleaning.
#
# Two driver tables live alongside this notebook:
#   tag_rules.csv       — formula-derived (code, criteria_json, measure)
#   code_dictionary.csv — code → (econ, econ_sub, func, func_sub) decomposition
#                         derived from the structured code naming. EXP_CROSS_*
#                         rows (econ × func intersections) are EXCLUDED to
#                         avoid double-counting against their EXP_ECON_* and
#                         EXP_FUNC_* parents.
#
# Anything that smells like interpretation gets reported in
# Zimbabwe/_analysis/ISSUES.md, never patched here.

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
COUNTRY = "Zimbabwe"
COUNTRY_MICRODATA_DIR = f"{WORKSPACE_DIR}/microdata_csv/{COUNTRY}"

# Driver CSVs are checked into the repo next to this notebook.
ANALYSIS_DIR = (Path(__file__).resolve().parent / "_analysis" / "data") \
    if "__file__" in globals() else Path("Zimbabwe/_analysis/data")

CSV_READ_OPTIONS = {
    "header": "true",
    "multiline": "true",
    "quote": '"',
    "escape": '"',
    "inferSchema": "true",
}

# Named-range → raw-column mapping (from Zimbabwe/_analysis/defined_names.csv).
EXP_NAMED_RANGE_TO_COL = {
    "year":     "year",
    "admin1":   "admin1",
    "econ1":    "econ1",
    "econ2":    "econ2",
    "econ3":    "econ3",
    "econ4":    "econ4",
    "prog1":    "prog1",
    "prog2":    "prog2",
    "prog3":    "prog3",
    "Exclude":  "Exclude",
    "type":     "type",
    "approved": "approved",
    "executed": "executed",
}
REV_NAMED_RANGE_TO_COL = {
    "year_r":     "year",
    "econ1_r":    "econ1",
    "econ2_r":    "econ2",
    "econ3_R":    "econ3",
    "econ4_r":    "econ4",
    "executed_r": "executed",
}


# ---------- Bronze: raw load, no transforms ----------

@dlt.table(name="zwe_expenditure_bronze",
           comment="Raw Expenditure sheet from Zimbabwe BOOST workbook. No cleaning.")
def expenditure_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/Expenditure.csv"))
    return df.withColumn("id", concat(lit("zwe_exp_"), monotonically_increasing_id()))


@dlt.table(name="zwe_revenue_bronze",
           comment="Raw Revenue sheet from Zimbabwe BOOST workbook. No cleaning.")
def revenue_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/Revenue.csv"))
    return df.withColumn("id", concat(lit("zwe_rev_"), monotonically_increasing_id()))


# ---------- Tag rule application ----------

def _criterion_to_sql(field_col: str, op: str, value: str) -> str:
    """Translate a single SUMIFS criterion to a Spark SQL boolean expression.
    op may be:  =, <>, <, <=, >, >=,  =year (cell-ref placeholder, handled by caller).
    The value may contain Excel wildcards (* trailing) — we map those to LIKE.
    """
    val_str = value.replace("'", "''")
    if "*" in val_str:
        like = val_str.replace("*", "%")
        cmp = "NOT LIKE" if op == "<>" else "LIKE"
        return f"lower({field_col}) {cmp} lower('{like}')"
    if op in ("=", "<>"):
        sql_op = "!=" if op == "<>" else "="
        # numeric-like values stay un-quoted to match int/float columns
        try:
            float(val_str)
            return f"cast({field_col} as string) {sql_op} '{val_str}'"
        except ValueError:
            return f"lower(cast({field_col} as string)) {sql_op} lower('{val_str}')"
    # numeric comparisons
    return f"cast({field_col} as double) {op} {val_str}"


def _apply_rule(df: DataFrame, criteria: list[dict],
                named_range_to_col: dict[str, str]) -> DataFrame:
    """Filter df by every criterion in `criteria`, returning the filtered rows.
    Criteria referencing the year cell are handled by the caller; here we drop
    them and the caller groups by year afterwards."""
    where_parts = []
    for c in criteria:
        if c["op"] == "=year":
            continue  # handled via groupBy(year) downstream
        col_name = named_range_to_col.get(c["field"])
        if col_name is None:
            # Unknown named range — surfaced in ISSUES.md, skip rule clause.
            continue
        where_parts.append(_criterion_to_sql(col_name, c["op"], c["value"]))
    if where_parts:
        df = df.where(" AND ".join(where_parts))
    return df


def _load_driver_csv(name: str) -> list[dict]:
    """CSV reader that works both locally (file system) and on Databricks
    volumes (where the repo path is mounted under /Workspace/Repos/...)."""
    import csv as _csv
    p = ANALYSIS_DIR / name
    with open(p) as f:
        return list(_csv.DictReader(f))


@dlt.table(name="zwe_expenditure_silver",
           comment="One row per (year, code) — applies each TAG rule's SUMIFS criteria to bronze Expenditure.")
def expenditure_silver():
    bronze = dlt.read("zwe_expenditure_bronze")
    rules = _load_driver_csv("tag_rules.csv")
    code_dict = {r["code"]: r for r in _load_driver_csv("code_dictionary.csv")}

    # Only Approved-sheet TAG rules with a measure of approved/executed and a
    # code present in code_dictionary (CROSS rows excluded by design).
    relevant = [
        r for r in rules
        if r["row_type"] == "TAG"
        and r["sheet"] == "Approved"
        and r["measure"] in ("approved", "executed")
        and r["code"] in code_dict
    ]

    per_rule_dfs = []
    for r in relevant:
        criteria = json.loads(r["criteria_json"])
        filtered = _apply_rule(bronze, criteria, EXP_NAMED_RANGE_TO_COL)
        agg = (filtered
               .groupBy(col("year").cast("int").alias("year"))
               .agg(_sum("approved").alias("approved"),
                    _sum("executed").alias("executed"))
               .withColumn("code", lit(r["code"])))
        per_rule_dfs.append(agg)

    if not per_rule_dfs:
        return bronze.limit(0).select(
            lit(None).cast("int").alias("year"),
            lit(None).cast("string").alias("code"),
            lit(None).cast("double").alias("approved"),
            lit(None).cast("double").alias("executed"),
        )
    silver = per_rule_dfs[0]
    for d in per_rule_dfs[1:]:
        silver = silver.unionByName(d)
    return silver.select("year", "code", "approved", "executed")


@dlt.table(name="zwe_revenue_silver",
           comment="One row per (year, code) — applies REV_* TAG rules to bronze Revenue.")
def revenue_silver():
    bronze = dlt.read("zwe_revenue_bronze")
    rules = _load_driver_csv("tag_rules.csv")
    code_dict = {r["code"]: r for r in _load_driver_csv("code_dictionary.csv")}
    relevant = [
        r for r in rules
        if r["row_type"] == "TAG"
        and r["sheet"] == "Approved"
        and r["measure"] == "executed_r"
        and r["code"] in code_dict
    ]

    per_rule_dfs = []
    for r in relevant:
        criteria = json.loads(r["criteria_json"])
        filtered = _apply_rule(bronze, criteria, REV_NAMED_RANGE_TO_COL)
        agg = (filtered
               .groupBy(col("year").cast("int").alias("year"))
               .agg(_sum("executed").alias("executed"))
               .withColumn("code", lit(r["code"])))
        per_rule_dfs.append(agg)

    if not per_rule_dfs:
        return bronze.limit(0).select(
            lit(None).cast("int").alias("year"),
            lit(None).cast("string").alias("code"),
            lit(None).cast("double").alias("executed"),
        )
    silver = per_rule_dfs[0]
    for d in per_rule_dfs[1:]:
        silver = silver.unionByName(d)
    return silver.select("year", "code", "executed")


# ---------- Gold: join with code_dictionary, emit boost_gold schema ----------

@dlt.table(name="zwe_boost_gold",
           comment="Zimbabwe BOOST expenditure gold. Schema matches cross_country_aggregate_dlt.boost_gold.")
def boost_gold():
    silver = dlt.read("zwe_expenditure_silver")
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
            .withColumn("country_name", lit("Zimbabwe"))
            # admin0/geo0/admin1/admin2 not derivable from formulas alone — see ISSUES.md.
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
# Phase 4 — regenerate the SME discrepancy review doc.
# Compares pipeline silver against the Excel Executed sheet at (year, code)
# granularity; preserves prior expert resolutions across runs.

from utils import input_excel_filename  # noqa: E402

review_path = build_review(
    country_code="zwe",
    excel_path=input_excel_filename(COUNTRY),
    pipeline_df=spark.table("zwe_expenditure_silver"),
    dimension_cols=["year", "code"],
    value_cols=["approved", "executed"],
    excel_sheet="Executed",
    threshold=0.05,
    pipeline_source_ref="Zimbabwe/ZWE_transform_load_raw_dlt.py:zwe_expenditure_silver",
)
print(f"Discrepancy review written → {review_path}")
