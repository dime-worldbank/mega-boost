# Databricks notebook source
"""Moldova BOOST DLT pipeline.

Raw microdata splits across three year-range sheets (`2006-15`, `2016-19`,
`2020-24`) with different schemas and bilingual filter values (English
pre-2016, Romanian 2016+). Each tag rule in `tag_rules.csv` carries a
`measure` field (`approved` / `approved_16` / `approved_20` etc.) that
dispatches the rule to the matching raw sheet. Revenue rows live in the
same sheets; 2016+ formulas add `econ0_* = "Revenues"` to distinguish
them. Pre-2016 has no `econ0` → no revenue coverage.

Silver uses per-row if-else tagging (Albania pattern): each raw row gets
`econ_code` + `func_code` from the first-matching tag rule in workbook
sheet-row order. Multi-SUMIFS formulas are OR'd across blocks. Rows
that pass the uniform filter but match no rule stay in silver with NULL
code (coverage holes). Gold left-joins `code_dictionary.csv`.

Gold is NOT aggregated — row count equals filtered bronze row count.
Each input line is preserved with its tags and admin remap.

Not modelled (documented gaps — surfaced by Phase 7 verification):
- Hidden `Raw2` sheet: ~40 formulas add supplements from a 7-column
  sheet (`admin6` / no `econ0`) whose schema doesn't union cleanly.
  Under-counts EXP_FUNC_WAT_SAN and a few EXP_CROSS codes.
- Cell-subtraction (`=SUMIFS(…) - C19`): parser drops the `-cell_ref`
  suffix, over-counts SOC_ASS, PUB_SAF, SOC_PRO.

Admin hierarchy (applied in gold):
  admin0 = Central / Regional / Other   (from raw admin1 flag)
  admin1 = raw admin2 entity    when admin0 = Regional (district/council)
  admin2 = raw admin2 entity    when admin0 = Central  (ministry/agency)
  geo0 = admin0, geo1 = admin1
Raw admin1 is just the Central/Local flag; the 142 distinct entity
names (district councils, ministries, committees) live in raw admin2.
"""
import json
from functools import lru_cache, reduce
from operator import and_, or_
from pathlib import Path

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, lit, lower, when
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)


COUNTRY = "Moldova"
MICRODATA_DIR = f"/Volumes/prd_mega/sboost4/vboost4/Workspace/microdata_csv/{COUNTRY}"
ANALYSIS_DIR = (Path(__file__).resolve().parent / "_analysis" / "data"
                if "__file__" in globals() else Path("Moldova/_analysis/data"))
# Explicit schemas for the raw CSVs — skips the full-file scan that
# `inferSchema=true` would run on the driver (≈1 GB / scan on the 186 MB
# 2016-19 sheet) and guarantees stable types across runs.
CSV_OPTS = {"header": "true", "multiline": "true", "quote": '"', "escape": '"'}
TRANSFER_KEEP = {"excluding transfers", "cu exceptia transferurilor"}

_NUMERIC = {"year": IntegerType(), "approved": DoubleType(),
            "executed": DoubleType(), "revised": DoubleType(),
            "adjusted": DoubleType()}


def _schema(cols: list[str]) -> StructType:
    return StructType([StructField(c, _NUMERIC.get(c, StringType())) for c in cols])


SCHEMA_BASE = _schema([
    "year", "admin1", "func1", "func2", "econ1", "econ2",
    "exp_type", "transfer", "approved", "adjusted", "executed",
])
SCHEMA_16 = _schema([
    "year", "admin1", "admin2", "func1", "func2", "func3",
    "econ0", "econ1", "econ2", "econ3", "econ4", "econ5", "econ6",
    "exp_type", "transfer", "fin_source1",
    "approved", "revised", "executed",
    "program1", "program2", "activity",
])
SCHEMA_20 = _schema([
    "year", "admin1", "admin2", "func1", "func2", "func3",
    "econ0", "econ1", "econ2", "econ3", "econ4", "econ5", "econ6",
    "exp_type", "transfer", "fin_source1",
    "approved", "adjusted", "executed",
])

# Named-range → raw-column mapping per year-range sheet. 2016-19 has
# `revised` + program/activity; 2020-24 has `adjusted` instead.
_BASE_FIELDS = ["year", "admin1", "func1", "func2", "econ1", "econ2",
                "exp_type", "transfer", "approved", "executed", "adjusted"]
_WIDE_FIELDS = ["year", "admin1", "admin2", "func1", "func2", "func3",
                "econ0", "econ1", "econ2", "econ3", "econ4", "econ5", "econ6",
                "exp_type", "transfer", "fin_source1", "approved", "executed"]
MAP_BASE = {f: f for f in _BASE_FIELDS}
MAP_16 = {**{f"{f}_16": f for f in _WIDE_FIELDS},
          "revised_16": "revised", "program1_16": "program1",
          "program2_16": "program2", "activity_16": "activity"}
MAP_20 = {**{f"{f}_20": f for f in _WIDE_FIELDS}, "adjusted_20": "adjusted"}

RANGES = {
    "base": {"csv": "2006-15.csv", "bronze": "mda_raw_2006_15_bronze",
             "map": MAP_BASE, "schema": SCHEMA_BASE, "transfer_key": "transfer",
             "econ0_key": None},
    "16":   {"csv": "2016-19.csv", "bronze": "mda_raw_2016_19_bronze",
             "map": MAP_16,   "schema": SCHEMA_16,   "transfer_key": "transfer_16",
             "econ0_key": "econ0_16"},
    "20":   {"csv": "2020-24.csv", "bronze": "mda_raw_2020_24_bronze",
             "map": MAP_20,   "schema": SCHEMA_20,   "transfer_key": "transfer_20",
             "econ0_key": "econ0_20"},
}


def _classify(measure: str) -> str | None:
    if measure in ("approved", "executed"): return "base"
    if measure.endswith("_16"): return "16"
    if measure.endswith("_20"): return "20"
    return None


# ---------- driver CSVs (cached) ----------

def _load_csv(name: str) -> list[dict]:
    import csv as _csv
    with open(ANALYSIS_DIR / name) as f:
        return list(_csv.DictReader(f))


@lru_cache(maxsize=None)
def _rules() -> list[dict]:       return _load_csv("tag_rules.csv")


@lru_cache(maxsize=None)
def _code_dict() -> list[dict]:   return _load_csv("code_dictionary.csv")


@lru_cache(maxsize=None)
def _allowed() -> frozenset:      return frozenset(r["code"] for r in _code_dict())


def _rules_for(rk: str, prefix: str) -> list[dict]:
    """TAG rules dispatched to range `rk` whose code has the given prefix and
    is present in code_dictionary (excludes CROSS and meta-rollups).
    Ordered by workbook sheet row — the SME's authoritative priority."""
    allowed = _allowed()
    rules = [r for r in _rules()
             if r["row_type"] == "TAG" and r["sheet"] == "Approved"
             and _classify(r["measure"]) == rk
             and r["code"].startswith(prefix) and r["code"] in allowed]
    return sorted(rules, key=lambda r: int(r["row"]))


# ---------- cascade builder ----------

def _criterion_expr(col_name: str, op: str, value):
    """Translate one SUMIFS criterion into a Spark Column bool expression."""
    if op in ("in", "not in"):
        e = lower(col(col_name).cast("string")).isin([str(v).lower() for v in value])
        return ~e if op == "not in" else e
    s = str(value)
    if "*" in s:
        e = lower(col(col_name)).like(s.replace("*", "%").lower())
        return ~e if op == "<>" else e
    if op in ("=", "<>"):
        try:
            float(s)
            e = col(col_name).cast("string") == s
        except ValueError:
            e = lower(col(col_name).cast("string")) == s.lower()
        return ~e if op == "<>" else e
    v = float(s); c = col(col_name).cast("double")
    return {"<": c < v, "<=": c <= v, ">": c > v, ">=": c >= v}[op]


def _compact(groups: list[list[dict]]) -> list[list[dict]]:
    """Array-constant expansion → `in`/`not in` branch when branches differ
    only at one `=` / `<>` position. Cuts Spark plan size for rules like
    `SUMIFS(..., econ2, {"a","b","c","d"})`."""
    if len(groups) <= 1 or len({len(b) for b in groups}) != 1:
        return groups
    n = len(groups[0])
    varying = []
    for i in range(n):
        fields = {b[i]["field"] for b in groups}
        ops    = {b[i]["op"]    for b in groups}
        values = [b[i]["value"] for b in groups]
        if len(fields) != 1 or len(ops) != 1: return groups
        if len(set(values)) > 1: varying.append((i, values))
    if len(varying) != 1 or groups[0][varying[0][0]]["op"] not in ("=", "<>"):
        return groups
    k, values = varying[0]
    base = groups[0][k]
    compacted = list(groups[0])
    compacted[k] = {"field": base["field"],
                    "op": "in" if base["op"] == "=" else "not in",
                    "value": list(values)}
    return [compacted]


def _rule_expr(groups: list[list[dict]], mapping: dict):
    """OR across SUMIFS blocks, AND within each block. None if no block is
    evaluable under `mapping` (e.g. Raw2 fields in this range)."""
    branches = []
    for branch in _compact(groups):
        parts, ok = [], True
        for c in branch:
            if c["op"] == "=year": continue
            cn = mapping.get(c["field"])
            if cn is None: ok = False; break
            parts.append(_criterion_expr(cn, c["op"], c["value"]))
        if ok and parts:
            branches.append(reduce(and_, parts))
    return reduce(or_, branches) if branches else None


def _cascade(df: DataFrame, rules: list[dict], mapping: dict, out: str) -> DataFrame:
    """Flat CASE WHEN cascade: first matching rule wins, NULL otherwise.
    Nested `when().otherwise(when())` would bloat codegen on small drivers."""
    case = None
    for r in rules:
        m = _rule_expr(json.loads(r["criteria_json"]), mapping)
        if m is None: continue
        case = when(m, lit(r["code"])) if case is None else case.when(m, lit(r["code"]))
    null_str = lit(None).cast("string")
    return df.withColumn(out, null_str if case is None
                         else case.otherwise(null_str))


# ---------- bronze / silver factories ----------

def _bronze(rk: str) -> DataFrame:
    """Load raw CSV with a pinned schema — no driver-side inferSchema pass,
    no synthetic `id` column (gold doesn't need one)."""
    cfg = RANGES[rk]
    return (spark.read.format("csv").options(**CSV_OPTS)
            .schema(cfg["schema"])
            .load(f"{MICRODATA_DIR}/{cfg['csv']}"))


def _silver(rk: str, kind: str) -> DataFrame | None:
    """kind = 'EXP' or 'REV'. Returns None for ranges without revenue (base)."""
    cfg = RANGES[rk]
    if kind == "REV" and cfg["econ0_key"] is None:
        return None
    mp = cfg["map"]
    df = dlt.read(cfg["bronze"]).filter(
        lower(col(mp[cfg["transfer_key"]])).isin(list(TRANSFER_KEEP))
    )
    if cfg["econ0_key"]:
        target = "expenditures" if kind == "EXP" else "revenues"
        df = df.filter(lower(col(mp[cfg["econ0_key"]])) == target)
    df = df.persist()  # econ + func cascades share one filtered pass
    if kind == "EXP":
        df = _cascade(df, _rules_for(rk, "EXP_ECON_"), mp, "econ_code")
        df = _cascade(df, _rules_for(rk, "EXP_FUNC_"), mp, "func_code")
    else:
        df = _cascade(df, _rules_for(rk, "REV_ECON_"), mp, "econ_code")
    return df


# ---------- bronze tables ----------

@dlt.table(name="mda_raw_2006_15_bronze")
def b_base(): return _bronze("base")

@dlt.table(name="mda_raw_2016_19_bronze")
def b_16():   return _bronze("16")

@dlt.table(name="mda_raw_2020_24_bronze")
def b_20():   return _bronze("20")


# ---------- per-range silver tables ----------
# Split by range so DLT plans each independently; a single combined silver
# built the whole cascade plan on the driver, which OOM'd modest clusters.

@dlt.table(name="mda_expenditure_silver_base")
def s_exp_base(): return _silver("base", "EXP")

@dlt.table(name="mda_expenditure_silver_16")
def s_exp_16():   return _silver("16", "EXP")

@dlt.table(name="mda_expenditure_silver_20")
def s_exp_20():   return _silver("20", "EXP")

@dlt.table(name="mda_revenue_silver_16")
def s_rev_16():   return _silver("16", "REV")

@dlt.table(name="mda_revenue_silver_20")
def s_rev_20():   return _silver("20", "REV")


# ---------- gold ----------

def _admin0():
    lc = lower(col("admin1"))
    return (when(lc.isin(["central", "centrale", "centrala"]), lit("Central"))
            .when(lc.isin(["local", "locale"]), lit("Regional"))
            .otherwise(col("admin1")))


@dlt.table(
    name="mda_boost_gold",
    comment="Moldova BOOST expenditure gold — one row per raw expenditure "
            "record with econ/func labels via code_dictionary join. Schema "
            "matches cross_country_aggregate_dlt.boost_gold.",
)
def boost_gold():
    silver = (dlt.read("mda_expenditure_silver_base")
              .unionByName(dlt.read("mda_expenditure_silver_16"), allowMissingColumns=True)
              .unionByName(dlt.read("mda_expenditure_silver_20"), allowMissingColumns=True))
    rows = _code_dict()
    econ_df = spark.createDataFrame([
        {"econ_code": r["code"], "econ": r["econ"] or None,
         "econ_sub": r["econ_sub"] or None}
        for r in rows if r["tag_kind"] in ("EXP_ECON", "REV_ECON")
    ])
    func_df = spark.createDataFrame([
        {"func_code": r["code"], "func": r["func"] or None,
         "func_sub": r["func_sub"] or None}
        for r in rows if r["tag_kind"] == "EXP_FUNC"
    ])
    # Lookup tables are tiny (~60 rows) — broadcast-join so Spark skips the
    # shuffle it would otherwise do on the per-row silver (~1.36M rows).
    # Explicit types on the final select: cross_country_aggregate_dlt.boost_gold
    # asserts `approved` and `executed` are DoubleType and breaks otherwise.
    return (silver
            .join(broadcast(econ_df), "econ_code", "left")
            .join(broadcast(func_df), "func_code", "left")
            .withColumn("country_name", lit("Moldova"))
            .withColumn("admin0", _admin0())
            # Admin1/2 carry the entity name (district or ministry) from raw
            # admin2; raw admin1 only held the Central/Local flag.
            .withColumn("_entity", col("admin2"))
            .withColumn("admin1", when(col("admin0") == "Regional",
                                       col("_entity")).otherwise(lit(None).cast("string")))
            .withColumn("admin2", when(col("admin0") == "Central",
                                       col("_entity")).otherwise(lit(None).cast("string")))
            .withColumn("geo0", col("admin0"))
            .withColumn("geo1", col("admin1"))
            .drop("_entity")
            .select(
                col("country_name").cast("string"),
                col("year").cast("int"),
                col("admin0").cast("string"),
                col("admin1").cast("string"),
                col("admin2").cast("string"),
                col("geo0").cast("string"),
                col("geo1").cast("string"),
                col("func").cast("string"),
                col("func_sub").cast("string"),
                col("econ").cast("string"),
                col("econ_sub").cast("string"),
                col("approved").cast("double"),
                col("revised").cast("double"),
                col("executed").cast("double"),
            ))
