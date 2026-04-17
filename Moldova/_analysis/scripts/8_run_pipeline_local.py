"""EXPERIMENTAL — run the Moldova transform pipeline locally in pandas.

Mirrors `Moldova/MDA_transform_load_raw_dlt.py` exactly:
  bronze (read raw sheets from temp/<Country> BOOST.xlsx)
  → uniform filter (transfer + econ0)
  → per-range if-else cascade (sub-level codes first, sheet order tiebreaker)
  → union across ranges
  → join code_dictionary for labels
  → admin remap (admin0 from raw admin1 flag; admin1/2 split by admin0)
  → write local_gold.csv

Output: `_analysis/reports/local_gold.csv` (same columns as
mda_boost_gold). Row count should match what the DLT pipeline produces.

Usage:
    python3 scripts/8_run_pipeline_local.py

Scope:
- Expenditure silver only (matches `mda_boost_gold`). Revenue path is
  implemented but not written to CSV (toggle `WRITE_REV` below).
- Reads the Excel workbook directly — skips the bronze CSV extract
  step. If temp/ doesn't have the workbook, point `XLSX` at your copy.

NOT intended for production. No DLT, no Spark, no Delta. For fast
iteration on cascade priority, filter logic, admin mapping, or
offline inspection of the gold output.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent          # Moldova/_analysis
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
XLSX = ROOT.parent.parent / "temp" / "Moldova BOOST.xlsx"
REPORTS.mkdir(parents=True, exist_ok=True)

WRITE_REV = False  # flip to also write local_revenue_gold.csv

# ---------- mappings (keep in sync with MDA_transform_load_raw_dlt.py) ----------

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
    "base": {"sheet": "2006-15", "map": MAP_BASE,
             "transfer_key": "transfer", "econ0_key": None},
    "16":   {"sheet": "2016-19", "map": MAP_16,
             "transfer_key": "transfer_16", "econ0_key": "econ0_16"},
    "20":   {"sheet": "2020-24", "map": MAP_20,
             "transfer_key": "transfer_20", "econ0_key": "econ0_20"},
}
TRANSFER_KEEP = {"excluding transfers", "cu exceptia transferurilor"}


def _classify(measure: str) -> str | None:
    if measure in ("approved", "executed"): return "base"
    if measure.endswith("_16"): return "16"
    if measure.endswith("_20"): return "20"
    return None


# ---------- cascade (pandas version of the DLT helpers) ----------

def _criterion_mask(series: pd.Series, op: str, value) -> pd.Series:
    if op in ("in", "not in"):
        lowered = [str(v).lower() for v in value]
        m = series.astype(str).str.lower().isin(lowered)
        return ~m if op == "not in" else m
    s = str(value)
    if "*" in s:
        patt = "^" + s.replace("*", ".*") + "$"
        m = series.astype(str).str.match(patt, case=False, na=False)
        return ~m if op == "<>" else m
    if op in ("=", "<>"):
        try:
            float(s)
            eq = series.astype(str) == s
        except ValueError:
            eq = series.astype(str).str.lower() == s.lower()
        return ~eq if op == "<>" else eq
    num = pd.to_numeric(series, errors="coerce")
    v = float(s)
    return {"<": num < v, "<=": num <= v, ">": num > v, ">=": num >= v}[op]


def _compact_branches(groups: list[list[dict]]) -> list[list[dict]]:
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
    out = list(groups[0])
    out[k] = {"field": base["field"],
              "op": "in" if base["op"] == "=" else "not in",
              "value": list(values)}
    return [out]


def _rule_mask(df: pd.DataFrame, groups: list[list[dict]], mapping: dict) -> pd.Series:
    combined = pd.Series(False, index=df.index)
    for branch in _compact_branches(groups):
        parts, ok = [], True
        for c in branch:
            if c["op"] == "=year": continue
            cn = mapping.get(c["field"])
            if cn is None or cn not in df.columns:
                ok = False; break
            parts.append(_criterion_mask(df[cn], c["op"], c["value"]))
        if ok and parts:
            m = parts[0]
            for p in parts[1:]:
                m &= p
            combined |= m
    return combined


def _apply_cascade(df: pd.DataFrame, rules: list[dict], mapping: dict, out_col: str) -> pd.DataFrame:
    assigned = pd.Series([None] * len(df), index=df.index, dtype=object)
    for r in rules:
        mask = _rule_mask(df, json.loads(r["criteria_json"]), mapping)
        unset = assigned.isna()
        assigned = assigned.where(~(unset & mask), r["code"])
    df = df.copy()
    df[out_col] = assigned
    return df


def _rules_for(all_rules, rk: str, prefix: str, allowed: set, has_sub: dict) -> list[dict]:
    rules = [r for r in all_rules
             if r["row_type"] == "TAG" and r["sheet"] == "Approved"
             and _classify(r["measure"]) == rk
             and r["code"].startswith(prefix) and r["code"] in allowed]
    return sorted(rules, key=lambda r: (not has_sub.get(r["code"], False), int(r["row"])))


# ---------- pipeline ----------

def main() -> None:
    if not XLSX.exists():
        print(f"ERROR: workbook not found at {XLSX}", file=sys.stderr)
        print("Either put it there or edit the XLSX constant at the top.", file=sys.stderr)
        sys.exit(1)

    # driver CSVs
    with (DATA / "tag_rules.csv").open() as f:
        all_rules = list(csv.DictReader(f))
    with (DATA / "code_dictionary.csv").open() as f:
        code_dict = list(csv.DictReader(f))
    allowed = {r["code"] for r in code_dict}
    has_sub: dict[str, bool] = {}
    for r in code_dict:
        k = r["tag_kind"]
        if k in ("EXP_ECON", "REV_ECON"): has_sub[r["code"]] = bool(r.get("econ_sub"))
        elif k == "EXP_FUNC":             has_sub[r["code"]] = bool(r.get("func_sub"))
        else:                              has_sub[r["code"]] = False

    # bronze (read raw sheets directly from the workbook)
    bronzes: dict[str, pd.DataFrame] = {}
    for rk, cfg in RANGES.items():
        print(f"[{rk}] reading {cfg['sheet']}…")
        bronzes[rk] = pd.read_excel(XLSX, sheet_name=cfg["sheet"])
        print(f"[{rk}]   {len(bronzes[rk]):,} rows, {len(bronzes[rk].columns)} cols")

    def _silver(rk: str, kind: str) -> pd.DataFrame | None:
        cfg = RANGES[rk]
        if kind == "REV" and cfg["econ0_key"] is None:
            return None
        df = bronzes[rk]
        mp = cfg["map"]
        mask = df[mp[cfg["transfer_key"]]].astype(str).str.lower().isin(TRANSFER_KEEP)
        if cfg["econ0_key"]:
            tgt = "expenditures" if kind == "EXP" else "revenues"
            mask &= df[mp[cfg["econ0_key"]]].astype(str).str.lower() == tgt
        df = df[mask].copy()
        if kind == "EXP":
            df = _apply_cascade(df, _rules_for(all_rules, rk, "EXP_ECON_", allowed, has_sub), mp, "econ_code")
            df = _apply_cascade(df, _rules_for(all_rules, rk, "EXP_FUNC_", allowed, has_sub), mp, "func_code")
        else:
            df = _apply_cascade(df, _rules_for(all_rules, rk, "REV_ECON_", allowed, has_sub), mp, "econ_code")
        print(f"[{rk} {kind}]   silver: {len(df):,} rows")
        return df

    exp_frames = [_silver(rk, "EXP") for rk in RANGES]
    exp_silver = pd.concat(exp_frames, ignore_index=True, sort=False)
    print(f"Expenditure silver union: {len(exp_silver):,} rows")

    # ---------- gold ----------
    econ_lookup = {r["code"]: (r.get("econ") or None, r.get("econ_sub") or None)
                   for r in code_dict if r["tag_kind"] in ("EXP_ECON", "REV_ECON")}
    func_lookup = {r["code"]: (r.get("func") or None, r.get("func_sub") or None)
                   for r in code_dict if r["tag_kind"] == "EXP_FUNC"}

    exp_silver["econ"]     = exp_silver["econ_code"].map(lambda c: econ_lookup.get(c, (None, None))[0])
    exp_silver["econ_sub"] = exp_silver["econ_code"].map(lambda c: econ_lookup.get(c, (None, None))[1])
    exp_silver["func"]     = exp_silver["func_code"].map(lambda c: func_lookup.get(c, (None, None))[0])
    exp_silver["func_sub"] = exp_silver["func_code"].map(lambda c: func_lookup.get(c, (None, None))[1])

    def _admin0(v):
        s = str(v).lower() if pd.notna(v) else None
        if s in ("central", "centrale", "centrala"): return "Central"
        if s in ("local", "locale"):                  return "Regional"
        return v
    exp_silver["admin0"] = exp_silver["admin1"].map(_admin0)
    raw_admin2 = exp_silver["admin2"] if "admin2" in exp_silver.columns else pd.Series([None]*len(exp_silver))
    exp_silver["admin1"] = raw_admin2.where(exp_silver["admin0"] == "Regional", None)
    exp_silver["admin2"] = raw_admin2.where(exp_silver["admin0"] == "Central",  None)
    exp_silver["country_name"] = "Moldova"
    exp_silver["geo0"] = exp_silver["admin0"]
    exp_silver["geo1"] = exp_silver["admin1"]
    if "revised" not in exp_silver.columns:
        exp_silver["revised"] = pd.NA

    gold_cols = ["country_name", "year",
                 "admin0", "admin1", "admin2", "geo0", "geo1",
                 "func", "func_sub", "econ", "econ_sub",
                 "approved", "revised", "executed"]
    gold = exp_silver[gold_cols]
    out = REPORTS / "local_gold.csv"
    gold.to_csv(out, index=False)
    print(f"\nWrote {out} ({len(gold):,} rows)")
    print(f"  by admin0: {dict(gold['admin0'].value_counts())}")
    print(f"  NULL econ_code rows:  {exp_silver['econ_code'].isna().sum():,}")
    print(f"  NULL func_code rows:  {exp_silver['func_code'].isna().sum():,}")
    print(f"  year range: {gold['year'].min()} … {gold['year'].max()}")

    if WRITE_REV:
        rev_frames = [_silver(rk, "REV") for rk in RANGES if _silver(rk, "REV") is not None]
        if rev_frames:
            rev = pd.concat(rev_frames, ignore_index=True, sort=False)
            rev.to_csv(REPORTS / "local_revenue_gold.csv", index=False)
            print(f"Wrote revenue gold ({len(rev):,} rows)")


if __name__ == "__main__":
    main()
