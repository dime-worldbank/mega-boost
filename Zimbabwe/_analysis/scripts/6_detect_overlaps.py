"""Raw-data peer-overlap detector for Zimbabwe BOOST tags.

For every pair of codes that sit at the same classification level (and share
the same parent), apply both codes' SUMIFS criteria to the raw Expenditure /
Revenue sheets and check whether any raw rows satisfy BOTH rules. Those rows
are the genuine double-count: a row that contributes to both tags would be
summed twice if someone added the two tags together.

Only overlaps are reported. Clean peers (no intersecting rows) are omitted.

For each detected overlap we list:
  - the pair of codes
  - which years the intersection rows fall into
  - which admin1 ministries / functions the intersection rows fall under
  - the intersection row count and intersection sum of approved + executed

Parent-child relationships (code X at econ-level vs code Y at econ_sub-level
under the same econ value) are NOT reported — they're the expected hierarchy.

Outputs:
  Zimbabwe/_analysis/overlap_report.md
  Zimbabwe/_analysis/overlap_detail.csv  — one row per (pair, year, admin1) slice
"""
from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import pandas as pd

XLSX = Path("/Users/ysuzuki2/Developer/mega-boost/temp/Zimbabwe BOOST.xlsx")
ROOT = Path("/Users/ysuzuki2/Developer/mega-boost/Zimbabwe/_analysis")
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
DATA.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)
TAG_RULES = DATA / "tag_rules.csv"
CODE_DICT = DATA / "code_dictionary.csv"

EXP_MAP = {"year": "year", "admin1": "admin1",
           "econ1": "econ1", "econ2": "econ2", "econ3": "econ3", "econ4": "econ4",
           "prog1": "prog1", "prog2": "prog2", "prog3": "prog3",
           "Exclude": "Exclude", "type": "type",
           "approved": "Approved", "executed": "Executed"}
REV_MAP = {"year_r": "year", "econ1_r": "econ1", "econ2_r": "econ2",
           "econ3_R": "econ3", "econ4_r": "econ4", "executed_r": "Executed"}

LEVELS = ["econ", "econ_sub", "func", "func_sub"]


# ---------- criterion application (pandas) ----------

def _cmp(series: pd.Series, op: str, value: str) -> pd.Series:
    if "*" in value:
        pattern = "^" + re.escape(value).replace(r"\*", ".*") + "$"
        m = series.astype(str).str.match(pattern, case=False, na=False)
        return ~m if op == "<>" else m
    if op in ("=", "<>"):
        eq = series.astype(str).str.lower() == str(value).lower()
        return ~eq if op == "<>" else eq
    s = pd.to_numeric(series, errors="coerce")
    v = float(value)
    return {"<": s < v, "<=": s <= v, ">": s > v, ">=": s >= v}[op]


def rule_mask(df: pd.DataFrame, criteria: list[dict], mapping: dict[str, str]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for c in criteria:
        if c["op"] == "=year":
            continue
        col = mapping.get(c["field"])
        if col is None or col not in df.columns:
            return pd.Series(False, index=df.index)
        mask &= _cmp(df[col], c["op"], c["value"])
    return mask


# ---------- peer grouping (same logic as before) ----------

def _is_null(v) -> bool:
    return v is None or v == "" or (isinstance(v, float) and math.isnan(v))


def peer_pairs(code_dict: pd.DataFrame, level: str) -> list[tuple[str, str, str]]:
    """Return list of (code_a, code_b, group_label). Parent-child pairs (where
    one code lives at a shallower level) are excluded."""
    bucket: dict[str, list[str]] = defaultdict(list)
    for code, row in code_dict.iterrows():
        e, es, f, fs = row["econ"], row["econ_sub"], row["func"], row["func_sub"]
        if level == "econ" and not _is_null(e) and _is_null(es):
            bucket[str(e)].append(code)
        elif level == "econ_sub" and not _is_null(es) and not _is_null(e):
            bucket[str(e)].append(code)
        elif level == "func" and not _is_null(f) and _is_null(fs):
            bucket[str(f)].append(code)
        elif level == "func_sub" and not _is_null(fs) and not _is_null(f):
            bucket[str(f)].append(code)
    pairs = []
    for label, codes in bucket.items():
        for a, b in combinations(sorted(codes), 2):
            pairs.append((a, b, label))
    return pairs


# ---------- load driver CSVs ----------

def load_code_dict() -> pd.DataFrame:
    with open(CODE_DICT) as f:
        rows = list(csv.DictReader(f))
    df = pd.DataFrame(rows)
    for c in LEVELS:
        df[c] = df[c].where(df[c].astype(bool), None)
    return df.set_index("code")


def load_tag_rules() -> dict[str, dict]:
    with open(TAG_RULES) as f:
        rows = list(csv.DictReader(f))
    # Approved-sheet TAG rows are the canonical rule set (Executed rows have
    # the same criteria modulo the measure).
    return {r["code"]: r for r in rows
            if r["row_type"] == "TAG" and r["sheet"] == "Approved"}


# ---------- overlap breakdown ----------

def slice_breakdown(df: pd.DataFrame, mask: pd.Series, measure_cols: list[str]) -> dict:
    """Given an intersection mask, return per-year and per-admin1 breakdown."""
    inter = df[mask].copy()
    if inter.empty:
        return {}
    for m in measure_cols:
        if m in inter.columns:
            inter[m] = pd.to_numeric(inter[m], errors="coerce")
    per_year = (inter.groupby(inter["year"], dropna=True)
                      .agg(rows=("year", "size"),
                           **{m: (m, "sum") for m in measure_cols})
                      .reset_index().rename(columns={"year": "year"}))
    admin_col = "admin1" if "admin1" in inter.columns else None
    per_admin = None
    if admin_col is not None:
        per_admin = (inter.groupby(admin_col, dropna=False)
                           .agg(rows=(admin_col, "size"),
                                **{m: (m, "sum") for m in measure_cols})
                           .sort_values("rows", ascending=False)
                           .reset_index())
    return {"inter_rows": len(inter), "per_year": per_year, "per_admin": per_admin}


def detect_overlaps(src_df: pd.DataFrame, mapping: dict[str, str],
                    measure_cols: list[str],
                    code_dict: pd.DataFrame, tag_rules: dict[str, dict],
                    which_prefix_levels: list[str]) -> list[dict]:  # noqa: ARG
    """(See module docstring.)
    """
    """Compute row masks for every relevant code, then check each peer pair
    across the given levels. Returns a list of overlap records."""
    # Precompute masks for codes whose tag rule references fields in this mapping.
    masks: dict[str, pd.Series] = {}
    for code, rule in tag_rules.items():
        crits = json.loads(rule["criteria_json"])
        # Rule belongs to this source iff every field is in the mapping.
        if not all(c["op"] == "=year" or c["field"] in mapping for c in crits):
            continue
        masks[code] = rule_mask(src_df, crits, mapping)

    records = []
    for level in which_prefix_levels:
        for a, b, group_label in peer_pairs(code_dict, level):
            if a not in masks or b not in masks:
                continue
            inter_mask = masks[a] & masks[b]
            if not inter_mask.any():
                continue
            bd = slice_breakdown(src_df, inter_mask, measure_cols)
            records.append({
                "level": level, "group": group_label,
                "code_a": a, "code_b": b,
                "name_a": (tag_rules.get(a) or {}).get("category", ""),
                "name_b": (tag_rules.get(b) or {}).get("category", ""),
                "formula_a": (tag_rules.get(a) or {}).get("sample_formula", ""),
                "formula_b": (tag_rules.get(b) or {}).get("sample_formula", ""),
                "sample_year": (tag_rules.get(a) or {}).get("sample_year", ""),
                "inter_rows": bd["inter_rows"],
                "per_year": bd["per_year"],
                "per_admin": bd["per_admin"],
            })
    return records


# ---------- rendering ----------

def _fmt_num(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:,.0f}"


def render(overlaps: list[dict]) -> str:
    L = ["# Zimbabwe raw-data peer overlaps\n"]
    L.append("Detected by applying each tag rule's SUMIFS criteria to the raw "
             "Expenditure / Revenue sheets and intersecting peer row-sets. "
             "Only overlaps are shown — peer pairs with no shared rows are "
             "silently excluded. Parent-child relationships are ignored.\n")
    if not overlaps:
        L.append("**No peer overlaps detected across any classification level.**")
        return "\n".join(L)

    # Human-friendly section headers — describe the peer relationship in words.
    level_phrase = {
        "econ":     "Top-level econ peers under",
        "econ_sub": "econ_sub peers sharing econ",
        "func":     "Top-level func peers under",
        "func_sub": "func_sub peers sharing func",
    }
    for o in overlaps:
        phrase = level_phrase.get(o["level"], o["level"])
        L.append(f"## {phrase} \"{o['group']}\"\n")
        L.append(f"**`{o['code_a']}`** — _{o['name_a']}_")
        L.append(f"**`{o['code_b']}`** — _{o['name_b']}_\n")
        L.append(f"**{o['inter_rows']:,} raw rows in common.**\n")
        if o["formula_a"] or o["formula_b"]:
            yr = f" (sample year {o['sample_year']})" if o.get("sample_year") else ""
            L.append(f"**Excel formulas{yr}**\n")
            L.append("```")
            L.append(f"{o['code_a']}:\n  {o['formula_a']}")
            L.append("")
            L.append(f"{o['code_b']}:\n  {o['formula_b']}")
            L.append("```\n")

        py = o["per_year"]
        if py is not None and not py.empty:
            extra_cols = [c for c in py.columns if c not in ("year", "rows")]
            L.append("**Years in the overlap**\n")
            L.append("| year | rows | " + " | ".join(extra_cols) + " |")
            L.append("|---:|---:|" + "|".join("---:" for _ in extra_cols) + "|")
            for _, r in py.iterrows():
                other = [_fmt_num(r[c]) for c in extra_cols]
                year_str = str(int(r["year"])) if pd.notna(r["year"]) else "—"
                L.append(f"| {year_str} | {int(r['rows']):,} | " + " | ".join(other) + " |")
            L.append("")

        pa = o["per_admin"]
        if pa is not None and not pa.empty:
            extra_cols = [c for c in pa.columns if c not in ("admin1", "rows")]
            L.append("**Top functions (admin1 ministries) that cause the overlap**\n")
            L.append("| admin1 / ministry | rows | " + " | ".join(extra_cols) + " |")
            L.append("|---|---:|" + "|".join("---:" for _ in extra_cols) + "|")
            for _, r in pa.head(10).iterrows():
                other = [_fmt_num(r[c]) for c in extra_cols]
                L.append(f"| {r['admin1']} | {int(r['rows']):,} | " + " | ".join(other) + " |")
            if len(pa) > 10:
                L.append(f"\n_…and {len(pa) - 10} more ministries._")
            L.append("")

        L.append("---\n")
    return "\n".join(L)


def main():
    print("Loading raw sheets…")
    exp = pd.read_excel(XLSX, sheet_name="Expenditure")
    rev = pd.read_excel(XLSX, sheet_name="Revenue")
    print(f"  Expenditure: {len(exp):,} rows; Revenue: {len(rev):,} rows")

    code_dict = load_code_dict()
    tag_rules = load_tag_rules()

    # Expenditure-side tag rules cover econ / econ_sub / func / func_sub.
    exp_overlaps = detect_overlaps(
        exp, EXP_MAP, ["Approved", "Executed"],
        code_dict, tag_rules,
        which_prefix_levels=LEVELS,
    )
    # Revenue-side rules only have econ / econ_sub classifications.
    rev_overlaps = detect_overlaps(
        rev, REV_MAP, ["Executed"],
        code_dict, tag_rules,
        which_prefix_levels=["econ", "econ_sub"],
    )
    overlaps = exp_overlaps + rev_overlaps

    md = render(overlaps)
    provenance = (
        "> Generated by `scripts/6_detect_overlaps.py` from the source workbook,\n"
        "> `data/tag_rules.csv`, and `data/code_dictionary.csv`.\n"
        "> Flat drill-down at `data/overlap_detail.csv`.\n"
        "> Rerun with `python3 scripts/6_detect_overlaps.py`.\n\n"
    )
    (REPORTS / "overlap_report.md").write_text(provenance + md)

    # flat csv for drill-down
    flat = []
    for o in overlaps:
        if o["per_year"] is None or o["per_year"].empty:
            continue
        for _, yr in o["per_year"].iterrows():
            flat.append({
                "level": o["level"], "group": o["group"],
                "code_a": o["code_a"], "code_b": o["code_b"],
                "year": yr.get("year"), "rows": yr.get("rows"),
                **{c: yr.get(c) for c in yr.index if c not in ("year", "rows")},
            })
    pd.DataFrame(flat).to_csv(DATA / "overlap_detail.csv", index=False)
    print(f"Wrote reports/overlap_report.md ({len(overlaps)} overlap pairs)")


if __name__ == "__main__":
    main()
