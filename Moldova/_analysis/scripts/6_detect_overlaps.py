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

ROOT = Path(__file__).resolve().parent.parent          # Moldova/_analysis
XLSX = ROOT.parent.parent / "temp" / "Moldova BOOST.xlsx"
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


def rule_mask(df: pd.DataFrame, criteria_groups: list[list[dict]],
              mapping: dict[str, str]) -> pd.Series:
    """A raw row matches the rule when at least one OR-branch (SUMIFS block)
    is fully satisfied. Each branch AND-combines its criteria; the branches
    are OR'd together."""
    if not criteria_groups:
        return pd.Series(False, index=df.index)
    combined = pd.Series(False, index=df.index)
    for branch in criteria_groups:
        branch_mask = pd.Series(True, index=df.index)
        for c in branch:
            if c["op"] == "=year":
                continue
            col = mapping.get(c["field"])
            if col is None or col not in df.columns:
                branch_mask = pd.Series(False, index=df.index)
                break
            branch_mask &= _cmp(df[col], c["op"], c["value"])
        combined |= branch_mask
    return combined


# ---------- peer grouping (same logic as before) ----------

def _is_null(v) -> bool:
    return v is None or v == "" or (isinstance(v, float) and math.isnan(v))


def is_subnational(code: str) -> bool:
    return "_SBN_" in code or code.startswith("SBN_")


def _pairs_sharing_level_value(
    code_dict: pd.DataFrame,
    include: "callable",
) -> dict[frozenset, list[dict]]:
    """For each level (econ, econ_sub, func, func_sub), pair every code that
    has a non-null value at that level with every other — regardless of whether
    they share the value, regardless of parent. The annotation `same_value`
    lets the renderer/SME tell apart:

      - *same-value overlap* (e.g. two codes both `econ=Wage bill`) → they're
        claimed to describe the same thing; overlap in raw rows is expected
      - *cross-category overlap* (e.g. `econ=Wage bill` vs `econ=Goods and
        services`) → mutual exclusivity is expected; overlap is a real
        double-count bug

    Same-depth filtering keeps the pairing meaningful at each level:
      - econ-level pairs: both codes must be at the rollup level (`econ_sub`
        null) — otherwise a rollup would be compared with its own child
      - econ_sub-level pairs: both codes must have `econ_sub` set — siblings
        (potentially under different parents), not parent↔child
      - same for func / func_sub."""
    shared: dict[frozenset, list[dict]] = defaultdict(list)

    def _add_level(level_name: str, parent_level: str, require_sub: bool):
        """Gather codes qualifying at this level and pair them pairwise."""
        sub_field = "econ_sub" if parent_level == "econ" else "func_sub"
        qualifying: list[tuple[str, str]] = []  # (code, value_at_parent_level)
        for code, row in code_dict.iterrows():
            if not include(code, row):
                continue
            p_val = row.get(parent_level)
            s_val = row.get(sub_field)
            if _is_null(p_val):
                continue
            if require_sub and _is_null(s_val):
                continue
            if not require_sub and not _is_null(s_val):
                continue
            # For rollup-level (require_sub=False) the "value" is the parent;
            # for sub-level (require_sub=True) the "value" is the sub itself.
            v = str(s_val) if require_sub else str(p_val)
            qualifying.append((code, v))
        for (a, va), (b, vb) in combinations(sorted(qualifying), 2):
            shared[frozenset((a, b))].append({
                "level": level_name,
                "value_a": va,
                "value_b": vb,
                "same_value": va == vb,
            })

    _add_level("econ",     "econ", require_sub=False)
    _add_level("econ_sub", "econ", require_sub=True)
    _add_level("func",     "func", require_sub=False)
    _add_level("func_sub", "func", require_sub=True)
    return shared


def all_pairs_by_kind(code_dict: pd.DataFrame) -> list[tuple[str, str, str, list]]:
    """Pairs of non-subnational codes that qualify at the same classification
    level — see `_pairs_sharing_level_value` for the full semantics. Returns
    `(a, b, tag_kind, annotations)` where annotations let render() tell
    same-value pairs apart from cross-category (overcounting) pairs."""
    include = lambda code, row: not is_subnational(code)  # noqa: E731
    shared = _pairs_sharing_level_value(code_dict, include)
    pairs: list[tuple[str, str, str, list]] = []
    for key, annotations in shared.items():
        a, b = sorted(key)
        kind_a = code_dict.loc[a].get("tag_kind") or ""
        kind_b = code_dict.loc[b].get("tag_kind") or ""
        # Pair only within the same tag_kind; a REV_ECON code vs an EXP_ECON
        # code accidentally sharing a level should never happen (different
        # vocabularies) but guard anyway.
        if kind_a != kind_b:
            continue
        pairs.append((a, b, kind_a, annotations))
    return pairs


def sbn_pairs(code_dict: pd.DataFrame) -> list[tuple[str, str, str, list]]:
    """Subnational pairs: SBN codes compared only against other SBN codes.
    Kept fully separate from the non-SBN primary report — no mixing of
    subnational and non-subnational categories."""
    include = lambda code, row: is_subnational(code)  # noqa: E731
    shared = _pairs_sharing_level_value(code_dict, include)
    pairs: list[tuple[str, str, str, list]] = []
    for key, annotations in shared.items():
        a, b = sorted(key)
        kind_a = code_dict.loc[a].get("tag_kind") or ""
        kind_b = code_dict.loc[b].get("tag_kind") or ""
        if kind_a != kind_b:
            continue
        pairs.append((a, b, kind_a, annotations))
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
    """Per-year overcount breakdown.

    For each measure (e.g. `approved`, `executed`) we report, per year:
      - `Σ overcounted`: sum of the measure over rows satisfying BOTH filters
        — this is the exact amount double-counted if code A and code B totals
        are added together
      - `rows overlap`: number of raw rows matched by both filters

    Also returned as summary stats (grand totals over all years):
      - `total_a`, `total_b`: each code's full raw-sheet total per measure
      - `total_overcounted`: intersection total per measure

    The grand totals let the SME scale the overcounted amount — e.g. `$500k
    overcounted out of $10B total` is different from `$500k out of $600k`."""
    work = df.copy()
    for m in measure_cols:
        if m in work.columns:
            work[m] = pd.to_numeric(work[m], errors="coerce")

    inter_mask = mask_a & mask_b
    inter = work[inter_mask]
    a = work[mask_a]
    b = work[mask_b]

    if inter.empty:
        return {"inter_rows": 0, "per_year": None}

    per_year = (inter.groupby("year", dropna=True)
                     .agg(rows_overlap=("year", "size"),
                          **{f"{m}_overcounted": (m, "sum") for m in measure_cols})
                     .reset_index()
                     .sort_values("year").reset_index(drop=True))

    totals = {
        "total_a": {m: float(a[m].sum()) if m in a.columns else None for m in measure_cols},
        "total_b": {m: float(b[m].sum()) if m in b.columns else None for m in measure_cols},
        "total_overcounted": {m: float(inter[m].sum()) if m in inter.columns else None
                              for m in measure_cols},
    }
    return {"inter_rows": int(inter_mask.sum()), "per_year": per_year,
            "measure_cols": measure_cols, "totals": totals}


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
        groups = json.loads(rule["criteria_json"])
        # Skip the rule here only if NO branch has any field in the current
        # mapping — rules that mix mapped + Raw2 fields can still contribute
        # via their mapped branches; rule_mask returns False for the unmapped
        # ones, which is the conservative thing.
        any_relevant = any(
            any(c["op"] == "=year" or c["field"] in mapping for c in branch)
            for branch in groups
        )
        if not any_relevant:
            continue
        masks[code] = rule_mask(src_df, groups, mapping)

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
            "totals": bd.get("totals"),
        })
    return records


# ---------- rendering ----------

def _fmt_num(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:,.0f}"


def _render_overlap(o: dict, L: list[str]) -> None:
    """Render a single overlap record. Each annotation records how the two
    codes compare at a given classification level (same value = expected /
    duplicate; different values = cross-category overcounting signal)."""
    shared = o.get("shared") or []
    parts = []
    for ann in shared:
        lvl, va, vb, same = ann["level"], ann["value_a"], ann["value_b"], ann["same_value"]
        if same:
            parts.append(f"same `{lvl}`=`{va}`")
        else:
            parts.append(f"**cross `{lvl}`** (`{va}` vs `{vb}`)")
    header = "; ".join(parts) if parts else "(no annotation)"
    L.append(f"### `{o['code_a']}` vs `{o['code_b']}` — {header}\n")
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
        totals = o.get("totals") or {}

        # Scale-context summary: each code's grand total + overcounted grand total.
        if totals:
            L.append("**Totals across all years** — scale context for the "
                     "overcounted amount:\n")
            L.append("| measure | Σ code A (full) | Σ code B (full) | "
                     "**Σ overcounted** | overcounted ÷ min(A,B) |")
            L.append("|---|---:|---:|---:|---:|")
            for m in measures:
                ta = totals.get("total_a", {}).get(m) or 0
                tb = totals.get("total_b", {}).get(m) or 0
                oc = totals.get("total_overcounted", {}).get(m) or 0
                denom = min(abs(ta), abs(tb)) if (ta or tb) else 0
                pct = f"{100 * oc / denom:.1f}%" if denom else "—"
                L.append(f"| {m} | {_fmt_num(ta)} | {_fmt_num(tb)} | "
                         f"**{_fmt_num(oc)}** | {pct} |")
            L.append("")

        # Per-year overcount table.
        L.append("**Per-year overcounted amount** — sum of each measure over "
                 "raw rows matching BOTH codes (the exact amount that would be "
                 "double-counted if code A + code B were added):\n")
        header = ["year", "rows in overlap"] + [f"Σ {m} overcounted" for m in measures]
        L.append("| " + " | ".join(header) + " |")
        L.append("|---:|---:|" + "|".join("---:" for _ in measures) + "|")
        for _, r in py.iterrows():
            year_str = str(int(r["year"])) if pd.notna(r["year"]) else "—"
            cells = [year_str, f"{int(r['rows_overlap']):,}"]
            for m in measures:
                cells.append(_fmt_num(r[f"{m}_overcounted"]))
            L.append("| " + " | ".join(cells) + " |")
        L.append("")


def render(overlaps: list[dict], sbn_overlaps: list[dict]) -> str:
    L = ["# Moldova formula overcounting report\n"]
    L.append("Detects raw-data rows double-counted by the Approved/Executed "
             "SUMIFS formulas. For each classification level (`econ`, "
             "`econ_sub`, `func`, `func_sub`) every pair of codes qualifying "
             "at that level has its SUMIFS filter applied to the matching "
             "year-range raw sheet (`2006-15`, `2016-19`, `2020-24`); the "
             "intersection is the set of raw rows that satisfy BOTH formulas "
             "at once — i.e. rows the workbook's formulas count twice. "
             "Two kinds of overlap are reported:\n"
             "\n"
             "- **Same-value pairs** — two codes share the same value at a "
             "level (e.g. both `econ=Wage bill`). If the overcounted amount "
             "equals each code's own total, the two filters are effectively "
             "identical — rename or drop one.\n"
             "- **Cross-category pairs** — two codes carry different values "
             "at the same level (e.g. `econ=Wage bill` vs `econ=Goods and "
             "services`, or `econ_sub=Basic wages` vs `econ_sub=Recurrent "
             "maintenance`). Those categories are meant to be mutually "
             "exclusive, so *any* intersection is an overcounting bug.\n"
             "\n"
             "Pair annotations like `cross econ (Wage bill vs Goods and "
             "services)` mark the cross-category case in bold; `same econ=X` "
             "marks the same-value case.\n")
    L.append("> _Reading the tables below:_ `Σ overcounted` is the sum of a "
             "raw measure (approved / executed) over rows matched by BOTH "
             "codes — i.e. the exact amount that would be double-counted if "
             "the two codes' totals were added. The scale-context table shows "
             "each code's full raw-sheet total alongside the overcounted "
             "amount and its ratio to the smaller total.\n")
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
            annotations = o.get("shared") or []
            shared_str = "; ".join(
                f"{a['level']}:{a['value_a']}={a['value_b']}" if a["same_value"]
                else f"{a['level']}:cross[{a['value_a']}|{a['value_b']}]"
                for a in annotations
            )
            any_cross = any(not a["same_value"] for a in annotations)
            for _, yr in o["per_year"].iterrows():
                row = {
                    "scope": scope,
                    "range": o.get("range_label", ""),
                    "tag_kind": o["tag_kind"],
                    "shared": shared_str,
                    "cross_category": any_cross,
                    "code_a": o["code_a"], "code_b": o["code_b"],
                    "year": yr.get("year"),
                    "rows_overlap": yr.get("rows_overlap"),
                }
                for m in (o.get("measure_cols") or []):
                    row[f"{m}_overcounted"] = yr.get(f"{m}_overcounted")
                flat.append(row)
    pd.DataFrame(flat).to_csv(DATA / "overlap_detail.csv", index=False)
    print(f"Wrote reports/overlap_report.md "
          f"({len(overlaps)} primary + {len(sbn_overlaps)} SBN pairs)")


if __name__ == "__main__":
    main()
