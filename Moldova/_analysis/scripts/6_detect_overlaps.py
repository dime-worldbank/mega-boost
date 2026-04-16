"""Raw-data peer-overlap detector for Moldova BOOST tags.

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
  Moldova/_analysis/overlap_report.md
  Moldova/_analysis/overlap_detail.csv  — one row per (pair, year, admin1) slice
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

XLSX = Path("/Users/ysuzuki2/Developer/mega-boost/temp/Moldova BOOST.xlsx")
ROOT = Path("/Users/ysuzuki2/Developer/mega-boost/Moldova/_analysis")
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
DATA.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)
TAG_RULES = DATA / "tag_rules.csv"
CODE_DICT = DATA / "code_dictionary.csv"

# Moldova raw data is split across three year-range sheets, each with its
# own named-range suffix. Each entry below binds a range-key to the sheet and
# the criterion-field → raw-column mapping used when a rule's `measure` points
# at that range. The `measures` list is the set of measure strings seen in
# tag_rules.csv that dispatch a rule to this sheet.
SHEET_MAPS = {
    "base": {
        "sheet": "2006-15",
        "label": "2006-2015",
        "measures": ["approved", "executed"],
        "mapping": {
            "year": "year", "admin1": "admin1",
            "func1": "func1", "func2": "func2",
            "econ1": "econ1", "econ2": "econ2",
            "exp_type": "exp_type", "transfer": "transfer",
            "approved": "approved", "executed": "executed",
            "adjusted": "adjusted",
        },
        "measure_cols": ["approved", "executed"],
    },
    "16": {
        "sheet": "2016-19",
        "label": "2016-2019",
        "measures": ["approved_16", "executed_16"],
        "mapping": {
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
        },
        "measure_cols": ["approved", "executed"],
    },
    "20": {
        "sheet": "2020-24",
        "label": "2020-2024",
        "measures": ["approved_20", "executed_20"],
        "mapping": {
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
        },
        "measure_cols": ["approved", "executed"],
    },
}


def classify_measure(measure: str) -> str | None:
    """Return the SHEET_MAPS key a rule's measure dispatches to, or None."""
    if measure in SHEET_MAPS["base"]["measures"]:
        return "base"
    if measure.endswith("_16"):
        return "16"
    if measure.endswith("_20"):
        return "20"
    return None  # Raw2 and other non-standard measures are skipped


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


def is_subnational(code: str) -> bool:
    return "_SBN_" in code or code.startswith("SBN_")


def _pairs_sharing_level_value(
    code_dict: pd.DataFrame,
    include: "callable",
) -> dict[frozenset, list[tuple[str, str]]]:
    """Bucket codes at each classification level by their (non-null) value and
    emit pairs within each bucket — but only between codes that sit at the
    *same depth*. Concretely:

      - econ-level pairs: both codes have `econ` set AND `econ_sub` null
      - econ_sub-level pairs: both codes have `econ_sub` set; bucketed by their
        shared parent `econ` value (so only actual siblings pair)
      - same for func / func_sub

    This avoids cross-depth pairs like `EXP_FUNC_EDU_EXE` (func-only) vs
    `EXP_FUNC_SEC_EDU_EXE` (func_sub-level child of Education)."""
    shared: dict[frozenset, list[tuple[str, str]]] = defaultdict(list)

    def _add_bucket(level_name, parent_level, sub_level, label_fmt):
        """`level_name` appears in the output ("econ", "func_sub", …).
        Codes qualify when `parent_level` is non-null; if `sub_level` is given
        the code must have a value there too (for the "_sub" level) or must
        NOT have one (for the rollup level). Bucketing is by `parent_level`."""
        bucket: dict[str, list[str]] = defaultdict(list)
        for code, row in code_dict.iterrows():
            if not include(code, row):
                continue
            p_val = row.get(parent_level)
            s_val = row.get(sub_level) if sub_level else None
            if _is_null(p_val):
                continue
            if sub_level is None:
                # Rollup-level bucket — code must have NO sub value set.
                sub_field = "econ_sub" if parent_level == "econ" else "func_sub"
                if not _is_null(row.get(sub_field)):
                    continue
            else:
                if _is_null(s_val):
                    continue
            bucket[str(p_val)].append(code)
        for value, codes in bucket.items():
            for a, b in combinations(sorted(codes), 2):
                shared[frozenset((a, b))].append((level_name, label_fmt.format(value=value)))

    _add_bucket("econ",     "econ", None,       "{value}")
    _add_bucket("econ_sub", "econ", "econ_sub", "siblings under econ=`{value}`")
    _add_bucket("func",     "func", None,       "{value}")
    _add_bucket("func_sub", "func", "func_sub", "siblings under func=`{value}`")
    return shared


def all_pairs_by_kind(code_dict: pd.DataFrame) -> list[tuple[str, str, str, list]]:
    """Pairs of non-subnational codes that share at least one classification
    value (same econ, same econ_sub, same func, or same func_sub). Categories
    are never crossed — e.g. a code with econ=Wage bill is never paired with a
    code whose econ is Capital expenditures. Returns `(a, b, tag_kind,
    shared_levels)` so render() can label each overlap with its shared level(s)."""
    include = lambda code, row: not is_subnational(code)  # noqa: E731
    shared = _pairs_sharing_level_value(code_dict, include)
    pairs: list[tuple[str, str, str, list]] = []
    for key, levels in shared.items():
        a, b = sorted(key)
        # tag_kind comes from code_dict; both codes share a classification
        # value, so by construction they have the same tag_kind.
        kind = code_dict.loc[a].get("tag_kind") or code_dict.loc[b].get("tag_kind") or ""
        pairs.append((a, b, kind, levels))
    return pairs


def sbn_pairs(code_dict: pd.DataFrame) -> list[tuple[str, str, str, list]]:
    """Subnational pairs: SBN codes compared against each other, and against
    their non-SBN counterparts when they share a classification value. Emitted
    separately so the main report isn't cluttered by expected admin-×-category
    subsets."""
    def include(code, row):
        return True  # include everyone, but...
    shared = _pairs_sharing_level_value(code_dict, include)
    pairs: list[tuple[str, str, str, list]] = []
    for key, levels in shared.items():
        a, b = sorted(key)
        # Keep only pairs where at least one side is subnational.
        if not (is_subnational(a) or is_subnational(b)):
            continue
        kind = code_dict.loc[a].get("tag_kind") or code_dict.loc[b].get("tag_kind") or ""
        pairs.append((a, b, kind, levels))
    return pairs


def shared_levels(a_row, b_row) -> list[tuple[str, str]]:
    """Return [(level, value), ...] where codes A and B have the same non-null
    value. Purely informational — lets render() annotate the overlap."""
    out = []
    for lvl in LEVELS:
        va = a_row.get(lvl) if hasattr(a_row, "get") else a_row[lvl]
        vb = b_row.get(lvl) if hasattr(b_row, "get") else b_row[lvl]
        if not _is_null(va) and not _is_null(vb) and str(va) == str(vb):
            out.append((lvl, str(va)))
    return out


# ---------- load driver CSVs ----------

def load_code_dict() -> pd.DataFrame:
    with open(CODE_DICT) as f:
        rows = list(csv.DictReader(f))
    df = pd.DataFrame(rows)
    for c in LEVELS:
        df[c] = df[c].where(df[c].astype(bool), None)
    return df.set_index("code")


def load_tag_rules_by_range() -> dict[str, dict[str, dict]]:
    """Return {range_key: {code: rule}} indexed by the sheet the rule's measure
    dispatches to. Approved-sheet rules are canonical (Executed share criteria
    modulo the measure). Moldova emits one rule per (code × year-range) so a
    single code may appear in multiple range buckets."""
    with open(TAG_RULES) as f:
        rows = list(csv.DictReader(f))
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        if r["row_type"] != "TAG" or r["sheet"] != "Approved":
            continue
        rk = classify_measure(r["measure"])
        if rk is None:
            continue
        # First rule wins per (range, code) — tag_rules.csv is already ordered
        # by row; shape-group splits within a range are rare but if they occur
        # we'd sum multiple rule-masks downstream if needed.
        out[rk].setdefault(r["code"], r)
    return out


# ---------- overlap breakdown ----------

def pair_breakdown(df: pd.DataFrame, mask_a: pd.Series, mask_b: pd.Series,
                   measure_cols: list[str]) -> dict:
    """Per-year breakdown comparing the two codes' totals.

    For each measure (e.g. `approved`, `executed`) we report, per year:
      - Σ measure over rows matching code A (its full total in the raw sheet)
      - Σ measure over rows matching code B
      - gap = Σ(A) − Σ(B)

    The gap carries its sign — positive means A is larger, negative means B
    is larger. Combined with the per-code totals the SME can see whether one
    code is a strict subset of the other."""
    work = df.copy()
    for m in measure_cols:
        if m in work.columns:
            work[m] = pd.to_numeric(work[m], errors="coerce")
    a = work[mask_a]
    b = work[mask_b]
    if a.empty and b.empty:
        return {"inter_rows": 0, "per_year": None}
    # Per-year totals for each code; outer-join so years present in either side appear.
    a_agg = (a.groupby("year", dropna=True)
               .agg(rows_a=("year", "size"),
                    **{f"{m}_a": (m, "sum") for m in measure_cols})
               .reset_index())
    b_agg = (b.groupby("year", dropna=True)
               .agg(rows_b=("year", "size"),
                    **{f"{m}_b": (m, "sum") for m in measure_cols})
               .reset_index())
    per_year = a_agg.merge(b_agg, on="year", how="outer").fillna(0)
    for m in measure_cols:
        per_year[f"{m}_gap"] = per_year[f"{m}_a"] - per_year[f"{m}_b"]
    per_year = per_year.sort_values("year").reset_index(drop=True)
    inter_rows = int((mask_a & mask_b).sum())
    return {"inter_rows": inter_rows, "per_year": per_year,
            "measure_cols": measure_cols}


def detect_overlaps(src_df: pd.DataFrame, mapping: dict[str, str],
                    measure_cols: list[str],
                    code_dict: pd.DataFrame, tag_rules: dict[str, dict],
                    pairs: list[tuple[str, str, str, list]]) -> list[dict]:
    """For each `(code_a, code_b, tag_kind, shared_levels)` pair, compute the
    raw-row intersection and emit a record if non-empty. Masks are cached per
    code. Codes without a rule in this range are silently skipped — the pair
    simply isn't measurable here."""
    masks: dict[str, pd.Series] = {}
    for code, rule in tag_rules.items():
        crits = json.loads(rule["criteria_json"])
        if not all(c["op"] == "=year" or c["field"] in mapping for c in crits):
            continue
        masks[code] = rule_mask(src_df, crits, mapping)

    records = []
    for a, b, tag_kind, shared in pairs:
        if a not in masks or b not in masks:
            continue
        inter_mask = masks[a] & masks[b]
        if not inter_mask.any():
            continue
        bd = pair_breakdown(src_df, masks[a], masks[b], measure_cols)
        records.append({
            "tag_kind": tag_kind,
            "shared": shared,
            "code_a": a, "code_b": b,
            "name_a": (tag_rules.get(a) or {}).get("category", ""),
            "name_b": (tag_rules.get(b) or {}).get("category", ""),
            "formula_a": (tag_rules.get(a) or {}).get("sample_formula", ""),
            "formula_b": (tag_rules.get(b) or {}).get("sample_formula", ""),
            "sample_year": (tag_rules.get(a) or {}).get("sample_year", ""),
            "inter_rows": bd["inter_rows"],
            "per_year": bd["per_year"],
            "measure_cols": bd.get("measure_cols", measure_cols),
        })
    return records


# ---------- rendering ----------

def _fmt_num(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:,.0f}"


def _render_overlap(o: dict, L: list[str]) -> None:
    """Render a single overlap record into list L (appending lines)."""
    shared = o.get("shared") or []
    if shared:
        parts = []
        for lvl, val in shared:
            if val.startswith("siblings under "):
                parts.append(val)  # already self-describing
            else:
                parts.append(f"same `{lvl}`=`{val}`")
        L.append(f"### `{o['code_a']}` vs `{o['code_b']}` — {'; '.join(parts)}\n")
    else:
        L.append(f"### `{o['code_a']}` vs `{o['code_b']}`\n")
    L.append(f"- **{o['code_a']}** — _{o['name_a']}_")
    L.append(f"- **{o['code_b']}** — _{o['name_b']}_")
    L.append(f"- **{o['inter_rows']:,} raw rows in the intersection.**\n")
    if o["formula_a"] or o["formula_b"]:
        yr = f" (sample year {o['sample_year']})" if o.get("sample_year") else ""
        L.append(f"**Excel formulas{yr}**")
        L.append("```")
        L.append(f"{o['code_a']}:\n  {o['formula_a']}")
        L.append("")
        L.append(f"{o['code_b']}:\n  {o['formula_b']}")
        L.append("```\n")

    py = o["per_year"]
    if py is not None and not py.empty:
        measures = o.get("measure_cols") or []
        L.append("**Per-year totals and gap** — each code's full raw-sheet "
                 f"total for the year; `gap = Σ {o['code_a']} − Σ {o['code_b']}`. "
                 "A positive gap means the first code is larger by that amount.\n")
        header = ["year"]
        for m in measures:
            header += [f"Σ {m} · A", f"Σ {m} · B", f"gap {m} (A−B)"]
        L.append("| " + " | ".join(header) + " |")
        L.append("|---:|" + "|".join("---:" for _ in range(len(header) - 1)) + "|")
        for _, r in py.iterrows():
            year_str = str(int(r["year"])) if pd.notna(r["year"]) else "—"
            cells = [year_str]
            for m in measures:
                cells.append(_fmt_num(r[f"{m}_a"]))
                cells.append(_fmt_num(r[f"{m}_b"]))
                cells.append(_fmt_num(r[f"{m}_gap"]))
            L.append("| " + " | ".join(cells) + " |")
        L.append("")


def render(overlaps: list[dict], sbn_overlaps: list[dict]) -> str:
    L = ["# Moldova raw-data overlaps\n"]
    L.append("Each pair below is two codes that share a value at some "
             "classification level — same `econ`, same `econ_sub`, same "
             "`func`, or same `func_sub`. Their SUMIFS criteria are applied "
             "to the matching year-range raw sheet (`2006-15`, `2016-19`, "
             "`2020-24`) and intersected; pairs with shared raw rows are "
             "listed. Codes in unrelated classifications (e.g. `econ=Wage "
             "bill` vs `econ=Capital expenditures`) are never compared.\n")
    L.append("Subnational `SBN_*` codes are evaluated in a separate section "
             "because they are cross-cutting (admin × category) rather than "
             "a standard econ/func breakdown.\n")

    # Primary section: non-SBN overlaps grouped by range × tag_kind.
    by_range_kind: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for o in overlaps:
        by_range_kind[(o["range_key"], o["tag_kind"])].append(o)

    total = len(overlaps)
    L.append(f"## Primary overlaps — {total} pair(s)\n")
    if not total:
        L.append("_No overlaps detected among non-subnational codes._\n")
    for rk in ("base", "16", "20"):
        if not any(k[0] == rk for k in by_range_kind):
            continue
        cfg = SHEET_MAPS[rk]
        L.append(f"\n### {cfg['label']} (raw sheet `{cfg['sheet']}`)\n")
        for tag_kind in ("EXP_ECON", "EXP_FUNC", "REV_ECON"):
            items = by_range_kind.get((rk, tag_kind), [])
            if not items:
                continue
            L.append(f"\n#### {tag_kind} — {len(items)} overlap(s)\n")
            for o in items:
                _render_overlap(o, L)

    # Subnational section.
    L.append(f"\n## Subnational (`SBN_*`) overlaps — {len(sbn_overlaps)} pair(s)\n")
    L.append("_SBN codes filter by `admin1=\"local\"` and are expected to be "
             "subsets of their non-SBN parents. Listed here so the SME can "
             "confirm the subset relationship is clean._\n")
    if not sbn_overlaps:
        L.append("_No SBN overlaps detected._\n")
    by_range_sbn: dict[str, list[dict]] = defaultdict(list)
    for o in sbn_overlaps:
        by_range_sbn[o["range_key"]].append(o)
    for rk in ("base", "16", "20"):
        items = by_range_sbn.get(rk, [])
        if not items:
            continue
        cfg = SHEET_MAPS[rk]
        L.append(f"\n### {cfg['label']} (raw sheet `{cfg['sheet']}`)\n")
        for o in items:
            _render_overlap(o, L)

    return "\n".join(L)


def main():
    code_dict = load_code_dict()
    rules_by_range = load_tag_rules_by_range()

    primary_pairs = all_pairs_by_kind(code_dict)
    subnat_pairs = sbn_pairs(code_dict)
    print(f"Candidate pairs — primary: {len(primary_pairs)}, subnational: {len(subnat_pairs)}")

    overlaps: list[dict] = []
    sbn_overlaps: list[dict] = []
    for range_key, cfg in SHEET_MAPS.items():
        rules = rules_by_range.get(range_key, {})
        if not rules:
            print(f"[{cfg['label']}] no rules dispatch here — skipping")
            continue
        print(f"[{cfg['label']}] loading raw sheet '{cfg['sheet']}' "
              f"({len(rules)} rules) — this can take a few minutes…")
        df = pd.read_excel(XLSX, sheet_name=cfg["sheet"])
        print(f"[{cfg['label']}]   {len(df):,} rows × {len(df.columns)} cols loaded")

        primary = detect_overlaps(
            df, cfg["mapping"], cfg["measure_cols"],
            code_dict, rules, primary_pairs,
        )
        sbn = detect_overlaps(
            df, cfg["mapping"], cfg["measure_cols"],
            code_dict, rules, subnat_pairs,
        )
        for o in primary:
            o["range_key"] = range_key
            o["range_label"] = cfg["label"]
        for o in sbn:
            o["range_key"] = range_key
            o["range_label"] = cfg["label"]
        overlaps.extend(primary)
        sbn_overlaps.extend(sbn)
        print(f"[{cfg['label']}]   primary overlaps: {len(primary)}, "
              f"subnational overlaps: {len(sbn)}")
        del df

    md = render(overlaps, sbn_overlaps)
    provenance = (
        "> Generated by `scripts/6_detect_overlaps.py` from the source workbook,\n"
        "> `data/tag_rules.csv`, and `data/code_dictionary.csv`.\n"
        "> Flat drill-down at `data/overlap_detail.csv`.\n"
        "> Rerun with `python3 scripts/6_detect_overlaps.py`.\n\n"
    )
    (REPORTS / "overlap_report.md").write_text(provenance + md)

    # flat csv for drill-down — combines primary + SBN overlaps with a scope flag
    flat = []
    for scope, src in (("primary", overlaps), ("subnational", sbn_overlaps)):
        for o in src:
            if o["per_year"] is None or o["per_year"].empty:
                continue
            shared_str = "; ".join(f"{lvl}={val}" for lvl, val in (o.get("shared") or []))
            for _, yr in o["per_year"].iterrows():
                row = {
                    "scope": scope,
                    "range": o.get("range_label", ""),
                    "tag_kind": o["tag_kind"],
                    "shared": shared_str,
                    "code_a": o["code_a"], "code_b": o["code_b"],
                    "year": yr.get("year"),
                    "rows_a": yr.get("rows_a"), "rows_b": yr.get("rows_b"),
                }
                for m in (o.get("measure_cols") or []):
                    row[f"{m}_a"] = yr.get(f"{m}_a")
                    row[f"{m}_b"] = yr.get(f"{m}_b")
                    row[f"{m}_gap"] = yr.get(f"{m}_gap")
                flat.append(row)
    pd.DataFrame(flat).to_csv(DATA / "overlap_detail.csv", index=False)
    print(f"Wrote reports/overlap_report.md "
          f"({len(overlaps)} primary + {len(sbn_overlaps)} SBN pairs)")


if __name__ == "__main__":
    main()
