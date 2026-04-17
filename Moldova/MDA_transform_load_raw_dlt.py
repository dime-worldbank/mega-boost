# Databricks notebook source
# Moldova BOOST DLT pipeline.
#
# Moldova splits raw microdata across three year-range sheets (`2006-15`,
# `2016-19`, `2020-24`) with different column schemas and bilingual filter
# values (English pre-2016, Romanian 2016+). Each tag rule in `tag_rules.csv`
# carries a `measure` field that dispatches the rule to the correct raw
# sheet:
#
#   measure ∈ {approved, executed}            → 2006-15 bronze
#   measure ∈ {approved_16, executed_16}      → 2016-19 bronze
#   measure ∈ {approved_20, executed_20}      → 2020-24 bronze
#
# Revenue rules live in the same raw sheets; 2016+ variants add an
# `econ0_* = "Revenues"` criterion to distinguish revenue rows. Pre-2016
# has no econ0 column and no revenue coverage in the workbook.
#
# NOT MODELLED (documented coverage gaps):
#
#   - Hidden `Raw2` sheet. A handful of tag rules (≈40 formulas) add a
#     `+ SUM(SUMIFS('Raw2'!$F:$F, …))` supplement to their primary SUMIFS,
#     pulling extra rows from a hidden 7-column sheet whose schema
#     (`admin6` instead of `admin1`, no `econ0`) doesn't cleanly union
#     with the three main raw sheets. The Raw2 branch's contribution is
#     silently dropped, which causes `EXP_FUNC_WAT_SAN_EXE` and a couple
#     of `EXP_CROSS_*` codes to undercount (see
#     `_analysis/reports/parsing_verification.md`). Resolution is either
#     SME-side (merge Raw2 upstream into the main sheets) or a pipeline
#     refactor to support a fourth bronze with its own mapping.
#
#   - Cell-subtraction (`=SUMIFS(…) - C19`). A few tag rows adjust the
#     SUMIFS result by subtracting a sibling cell's value to avoid
#     double-counting a specific sub-category. The parser captures the
#     SUMIFS but drops the `-cell_ref`, so silver overcounts for the
#     affected codes (SOC_ASS, PUB_SAF, SOC_PRO). Resolution: either
#     split these codes into two rules in the workbook, or add a cell-ref
#     evaluation step to the parser.
#
# SILVER — per-row if-else tagging (Albania pattern).
# ------------------------------------------------------------------
# Each raw expenditure row is tagged with:
#   - econ_code: first EXP_ECON_* tag rule whose SUMIFS criteria the row
#     satisfies, scanned in the workbook's sheet-row order
#   - func_code: first EXP_FUNC_* tag rule, same scan
# Revenue rows are similarly tagged with econ_code from REV_ECON_* rules.
#
# Multi-SUMIFS formulas (e.g. `=SUMIFS(…) + SUMIFS(…)`) are treated as OR:
# a row matches if ANY SUMIFS block's criteria are all satisfied. See
# `_analysis/data/tag_rules.csv` — `criteria_json` is a list-of-lists; each
# inner list is one SUMIFS block.
#
# We keep rows that pass the uniform filter (transfer filter + econ0) but
# don't match any rule — their code column is NULL so coverage holes are
# visible in silver. Gold is left-joined so NULL codes get NULL labels.
#
# Priority (code order) is intentional: overlaps between rules are listed
# in `_analysis/reports/overlap_report.md` for SME review, and the SME's
# choice of sheet-row order determines which code wins.
#
# Gold is NOT an aggregate. Row count in `mda_boost_gold` equals the
# filtered bronze row count: every input line item is preserved, tagged
# with its econ / econ_sub / func / func_sub labels (via code_dictionary
# join) and remapped admin0 / admin1 / admin2 / geo0 / geo1. Downstream
# consumers can group however they want.
#
# Admin hierarchy (Moldova-specific — see boost_gold for detail):
#   admin0 = Central / Regional / Other  (from raw admin1 flag)
#   admin1 = raw admin2 entity for Regional rows (district/council name)
#   admin2 = raw admin2 entity for Central rows  (ministry/agency name)
#   geo0   = admin0;   geo1 = admin1
# Raw admin1 ("Locale"/"Centrale"/"Local"/"Central"/"Other") is a coarse
# flag only; Moldova's actual entity names live in raw admin2.

import json
from functools import reduce
from operator import and_, or_
from pathlib import Path

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, lower, when, concat, monotonically_increasing_id,
)

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

# Named-range → raw-column mapping per year-range sheet. Keys appear in the
# Approved/Executed formulas; values are the raw CSV column headers.
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
    "base": {"bronze": "mda_raw_2006_15_bronze",  "mapping": MAP_BASE,
             "transfer_col": "transfer", "econ0_col": None},
    "16":   {"bronze": "mda_raw_2016_19_bronze",  "mapping": MAP_16,
             "transfer_col": "transfer_16", "econ0_col": "econ0_16"},
    "20":   {"bronze": "mda_raw_2020_24_bronze",  "mapping": MAP_20,
             "transfer_col": "transfer_20", "econ0_col": "econ0_20"},
}

# Uniform filter values: the `transfer` column values kept by every tag rule.
UNIFORM_TRANSFER_VALUES = {"excluding transfers", "cu exceptia transferurilor"}


def classify_measure(measure: str) -> str | None:
    """Return the range-key a rule's measure dispatches to (or None)."""
    if measure in ("approved", "executed"):
        return "base"
    if measure.endswith("_16"):
        return "16"
    if measure.endswith("_20"):
        return "20"
    return None


# ---------- Bronze ----------

@dlt.table(name="mda_raw_2006_15_bronze",
           comment="Raw 2006-15 sheet from Moldova BOOST workbook. No cleaning.")
def raw_2006_15_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/2006-15.csv"))
    return df.withColumn("id", concat(lit("mda_base_"), monotonically_increasing_id()))


@dlt.table(name="mda_raw_2016_19_bronze",
           comment="Raw 2016-19 sheet from Moldova BOOST workbook. No cleaning.")
def raw_2016_19_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/2016-19.csv"))
    return df.withColumn("id", concat(lit("mda_16_"), monotonically_increasing_id()))


@dlt.table(name="mda_raw_2020_24_bronze",
           comment="Raw 2020-24 sheet from Moldova BOOST workbook. No cleaning.")
def raw_2020_24_bronze():
    df = (spark.read.format("csv")
          .options(**CSV_READ_OPTIONS)
          .load(f"{COUNTRY_MICRODATA_DIR}/2020-24.csv"))
    return df.withColumn("id", concat(lit("mda_20_"), monotonically_increasing_id()))


# ---------- Tag-rule compilation helpers ----------

def _criterion_to_col_expr(col_name: str, op: str, value):
    """Translate a single SUMIFS criterion into a Spark Column boolean expr.
    Supports an extra `op` — `in` / `not in` — emitted by `_compact_branches`
    when an array-constant expansion can collapse to a single `isin`."""
    if op in ("in", "not in"):
        vals = [str(v).lower() for v in value]
        expr = lower(col(col_name).cast("string")).isin(vals)
        return ~expr if op == "not in" else expr
    val_str = str(value)
    if "*" in val_str:
        like = val_str.replace("*", "%").lower()
        expr = lower(col(col_name)).like(like)
        return ~expr if op == "<>" else expr
    if op in ("=", "<>"):
        try:
            float(val_str)
            expr = col(col_name).cast("string") == val_str
        except ValueError:
            expr = lower(col(col_name).cast("string")) == val_str.lower()
        return ~expr if op == "<>" else expr
    v = float(val_str)
    c = col(col_name).cast("double")
    return {"<": c < v, "<=": c <= v, ">": c > v, ">=": c >= v}[op]


def _compact_branches(criteria_groups: list[list[dict]]) -> list[list[dict]]:
    """Collapse array-constant expansion back into a single branch with an
    `in`/`not in` op when possible.

    `SUMIFS(..., econ2, {"a","b","c","d"})` enters this file as 4 branches
    that share all criteria except the value at one position. Keeping them
    as 4 separate OR'd AND-chains bloats the Spark expression tree and makes
    codegen slow / memory-hungry; Spark's `isin` is a single SQL predicate
    regardless of list length. We detect the pattern by checking that every
    branch has the same (field, op) at every position and that values differ
    at exactly one position — then rewrite that position to use `in`."""
    if len(criteria_groups) <= 1:
        return criteria_groups
    lengths = {len(b) for b in criteria_groups}
    if len(lengths) != 1:
        return criteria_groups
    n = lengths.pop()
    if n == 0:
        return criteria_groups
    varying: list[tuple[int, list]] = []
    for i in range(n):
        fields = {b[i]["field"] for b in criteria_groups}
        ops = {b[i]["op"] for b in criteria_groups}
        values = [b[i]["value"] for b in criteria_groups]
        if len(fields) != 1 or len(ops) != 1:
            return criteria_groups
        if len(set(values)) > 1:
            varying.append((i, values))
    if len(varying) != 1:
        return criteria_groups
    k, values = varying[0]
    base = criteria_groups[0][k]
    if base["op"] not in ("=", "<>"):
        return criteria_groups
    compacted = list(criteria_groups[0])
    compacted[k] = {
        "field": base["field"],
        "op": "in" if base["op"] == "=" else "not in",
        "value": list(values),
    }
    return [compacted]


def _rule_match_expr(criteria_groups: list[list[dict]], mapping: dict[str, str]):
    """OR across SUMIFS blocks; AND within a block. Returns a Column, or None
    if no block is fully evaluable (e.g. all fields reference a sheet not in
    this mapping — happens for rules dispatched to a different range, or the
    hidden `Raw2` sheet)."""
    criteria_groups = _compact_branches(criteria_groups)
    branch_exprs = []
    for branch in criteria_groups:
        and_parts = []
        ok = True
        for c in branch:
            if c["op"] == "=year":
                continue  # year handled by the raw sheet's own `year` column
            col_name = mapping.get(c["field"])
            if col_name is None:
                # Field lives outside this range's schema — drop the entire
                # branch (we cannot evaluate it without false positives).
                ok = False
                break
            and_parts.append(_criterion_to_col_expr(col_name, c["op"], c["value"]))
        if not ok or not and_parts:
            continue
        branch_exprs.append(reduce(and_, and_parts))
    if not branch_exprs:
        return None
    return reduce(or_, branch_exprs)


def _apply_cascade(df: DataFrame, rules: list[dict], mapping: dict[str, str],
                   out_col: str) -> DataFrame:
    """Build a single flat CASE WHEN ... WHEN ... ELSE NULL END for the rule
    cascade — one `when` per rule, in sheet-row order. A flat chain codegens
    to one Spark case expression; the equivalent nested `when().otherwise(
    when())` pattern produces a deep tree that can blow out codegen size on
    small clusters."""
    cascade = None
    for r in rules:
        match = _rule_match_expr(json.loads(r["criteria_json"]), mapping)
        if match is None:
            continue
        code_lit = lit(r["code"])
        cascade = when(match, code_lit) if cascade is None else cascade.when(match, code_lit)
    if cascade is None:
        return df.withColumn(out_col, lit(None).cast("string"))
    return df.withColumn(out_col, cascade.otherwise(lit(None).cast("string")))


def _load_driver_csv(name: str) -> list[dict]:
    import csv as _csv
    p = ANALYSIS_DIR / name
    with open(p) as f:
        return list(_csv.DictReader(f))


def _rules_for_range(all_rules: list[dict], rk: str, code_prefix: str,
                     allowed_codes: set[str]) -> list[dict]:
    """Rules for range `rk` whose code starts with `code_prefix` (EXP_ECON_ /
    EXP_FUNC_ / REV_ECON_) and whose code is present in the dictionary — i.e.
    not excluded as EXP_CROSS_* or meta-rollup. Ordered by workbook sheet row
    (the SME's authoritative priority order)."""
    out = [
        r for r in all_rules
        if r["row_type"] == "TAG"
        and r["sheet"] == "Approved"
        and classify_measure(r["measure"]) == rk
        and r["code"].startswith(code_prefix)
        and r["code"] in allowed_codes
    ]
    out.sort(key=lambda r: int(r["row"]))
    return out


def _uniform_filter(df: DataFrame, cfg: dict, econ0_target: str | None) -> DataFrame:
    """Apply the universal expenditure/revenue filter shared by every tag
    rule: `transfer` must be one of the accepted values; for 2016+ the
    `econ0` column must match the given target (`Expenditures` or
    `Revenues`). Rows failing this filter are dropped."""
    mapping = cfg["mapping"]
    transfer_logical = cfg["transfer_col"]
    transfer_col = mapping.get(transfer_logical)
    if transfer_col:
        df = df.filter(lower(col(transfer_col)).isin(list(UNIFORM_TRANSFER_VALUES)))
    econ0_logical = cfg["econ0_col"]
    if econ0_logical and econ0_target is not None:
        econ0_col = mapping.get(econ0_logical)
        if econ0_col:
            df = df.filter(lower(col(econ0_col)) == econ0_target.lower())
    return df


# ---------- Silver ----------

def _tagged_expenditure_per_range(rk: str, all_rules: list[dict],
                                   allowed_codes: set[str]) -> DataFrame:
    cfg = RANGE_CONFIG[rk]
    bronze = dlt.read(cfg["bronze"])
    # Persist the filtered frame so the two cascades (econ + func) share one
    # in-memory pass over the bronze rows instead of re-scanning.
    df = _uniform_filter(bronze, cfg, econ0_target="Expenditures").persist()
    mapping = cfg["mapping"]
    econ_rules = _rules_for_range(all_rules, rk, "EXP_ECON_", allowed_codes)
    func_rules = _rules_for_range(all_rules, rk, "EXP_FUNC_", allowed_codes)
    df = _apply_cascade(df, econ_rules, mapping, "econ_code")
    df = _apply_cascade(df, func_rules, mapping, "func_code")
    return df


def _tagged_revenue_per_range(rk: str, all_rules: list[dict],
                               allowed_codes: set[str]) -> DataFrame | None:
    cfg = RANGE_CONFIG[rk]
    if cfg["econ0_col"] is None:
        # 2006-15 has no econ0 column — workbook carries no revenue rows.
        return None
    bronze = dlt.read(cfg["bronze"])
    df = _uniform_filter(bronze, cfg, econ0_target="Revenues").persist()
    mapping = cfg["mapping"]
    econ_rules = _rules_for_range(all_rules, rk, "REV_ECON_", allowed_codes)
    df = _apply_cascade(df, econ_rules, mapping, "econ_code")
    return df


@dlt.table(name="mda_expenditure_silver",
           comment="One row per raw expenditure record with econ_code and "
                   "func_code assigned by first-matching tag rule in workbook "
                   "sheet-row order. Unmatched rows kept with NULL code.")
def expenditure_silver():
    all_rules = _load_driver_csv("tag_rules.csv")
    allowed = {r["code"] for r in _load_driver_csv("code_dictionary.csv")}
    per_range = []
    for rk in RANGE_CONFIG:
        per_range.append(_tagged_expenditure_per_range(rk, all_rules, allowed))
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), per_range)


@dlt.table(name="mda_revenue_silver",
           comment="One row per raw revenue record with econ_code assigned "
                   "by first-matching REV_ECON_* rule. Pre-2016 excluded "
                   "(workbook has no revenue rows before 2016).")
def revenue_silver():
    all_rules = _load_driver_csv("tag_rules.csv")
    allowed = {r["code"] for r in _load_driver_csv("code_dictionary.csv")}
    per_range = []
    for rk in RANGE_CONFIG:
        df = _tagged_revenue_per_range(rk, all_rules, allowed)
        if df is not None:
            per_range.append(df)
    if not per_range:
        # Defensive: no range carries revenue. Emit an empty frame with the
        # expected columns so downstream consumers don't break.
        return dlt.read(RANGE_CONFIG["16"]["bronze"]).limit(0) \
            .withColumn("econ_code", lit(None).cast("string"))
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), per_range)


# ---------- Gold ----------

def _code_dict_splits() -> tuple[DataFrame, DataFrame]:
    """Two label tables — one keyed on econ_code, one on func_code — derived
    from `code_dictionary.csv` split by `tag_kind`. Revenue and expenditure
    econ labels share the same table because both sit under `tag_kind`
    prefix `EXP_ECON`/`REV_ECON`."""
    rows = _load_driver_csv("code_dictionary.csv")
    econ_rows = [
        {"econ_code": r["code"], "econ": r["econ"] or None,
         "econ_sub": r["econ_sub"] or None}
        for r in rows if r["tag_kind"] in ("EXP_ECON", "REV_ECON")
    ]
    func_rows = [
        {"func_code": r["code"], "func": r["func"] or None,
         "func_sub": r["func_sub"] or None}
        for r in rows if r["tag_kind"] == "EXP_FUNC"
    ]
    return (spark.createDataFrame(econ_rows),
            spark.createDataFrame(func_rows))


def _derive_admin0(admin1_col: str):
    """Moldova raw admin1 is a coarse Central/Local/Other flag with era-
    specific spelling — Central/Local in 2006-15, Centrale/Locale in
    2016+. Collapse to the cross-country admin0 vocabulary (Central /
    Regional / Other)."""
    lc = lower(col(admin1_col))
    return (when(lc.isin(["central", "centrale", "centrala"]), lit("Central"))
            .when(lc.isin(["local", "locale"]), lit("Regional"))
            .otherwise(col(admin1_col)))


@dlt.table(name="mda_boost_gold",
           comment="Moldova BOOST expenditure gold — one row per raw "
                   "expenditure record with econ/func labels via "
                   "code_dictionary join. Schema matches "
                   "cross_country_aggregate_dlt.boost_gold.")
def boost_gold():
    silver = dlt.read("mda_expenditure_silver")
    econ_df, func_df = _code_dict_splits()

    # Admin mapping rationale:
    #   Moldova's raw admin1 is only ever Central/Local/Other — too coarse
    #   for the cross-country `admin1` slot (state/province name). The
    #   useful entity name (district council, ministry, committee — 142
    #   distinct values in 2016-19) lives in raw admin2. Split it by
    #   admin0 so the cross-country schema carries the right thing at the
    #   right level:
    #     * Regional rows → admin1 = raw admin2 (district/raion/UTAG)
    #     * Central rows  → admin2 = raw admin2 (ministry/agency name)
    #   2006-15 has no raw admin2, so admin1 and admin2 end up NULL there
    #   (the pre-2016 workbook doesn't carry entity-level detail).
    return (silver
            .join(econ_df, on="econ_code", how="left")
            .join(func_df, on="func_code", how="left")
            .withColumn("country_name", lit("Moldova"))
            .withColumn("admin0", _derive_admin0("admin1"))
            .withColumn("raw_admin2_entity", col("admin2"))
            .withColumn("admin1",
                when(col("admin0") == "Regional", col("raw_admin2_entity"))
                .otherwise(lit(None).cast("string")))
            .withColumn("admin2",
                when(col("admin0") == "Central", col("raw_admin2_entity"))
                .otherwise(lit(None).cast("string")))
            .withColumn("geo0", col("admin0"))
            .withColumn("geo1", col("admin1"))
            .drop("raw_admin2_entity")
            .select("country_name", "year",
                    "admin0", "admin1", "admin2", "geo0", "geo1",
                    "func", "func_sub", "econ", "econ_sub",
                    "approved", "revised", "executed"))
