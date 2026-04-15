"""Generate code_dictionary.csv — one row per non-CROSS TAG code with the
(econ, econ_sub, func, func_sub) mapping derived directly from the structured
code string. CROSS rows (econ × func intersections) are excluded for now since
they would double-count alongside the standalone EXP_ECON_* and EXP_FUNC_*
rows that fully cover them.

Also appends a "Suspicious tag rules" section to ISSUES.md flagging:
  - duplicate-criteria rules (two codes with identical filters)
  - cross-tabs whose admin1 doesn't match the named ministry in the code
"""
from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path("/Users/ysuzuki2/Developer/mega-boost/Zimbabwe/_analysis")
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
DATA.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)
TAG_RULES = DATA / "tag_rules.csv"
DICT_OUT  = DATA / "code_dictionary.csv"
ISSUES    = REPORTS / "ISSUES.md"

# Token dictionary derived by inspecting the 66 expenditure + 10 revenue codes.
# Multi-token tags (longer keys win during longest-match scan).
ECON_TOKENS = {
    "WAG_BIL":         ("Wage bill", None),
    "BAS_WAG":         ("Wage bill", "Basic wages"),
    "ALL":             ("Wage bill", "Allowances"),
    "PEN_CON":         ("Social benefits", "Pension contributions"),
    "SOC_ASS":         ("Social benefits", "Social assistance"),
    "SOC_BEN_PEN":     ("Social benefits", "Pensions"),
    "PEN":             ("Social benefits", "Pensions"),
    "USE_GOO_SER":     ("Goods and services", None),
    "GOO_SER_BAS_SER": ("Goods and services", "Basic services"),
    "GOO_SER":         ("Goods and services", None),
    "CAP_EXP":         ("Capital expenditures", None),
    "INT_DEB":         ("Interest on debt", None),
    "OTH_GRA":         ("Other grants/transfers", None),
    "SUB":             ("Subsidies", None),
    "TOT_EXP":         (None, None),  # grand total — no specific econ
    "WAG":             ("Wage bill", None),
    "COR_LOW_RIG":     ("Correction", "Low rigidity"),
    "COR_HIG_RIG":     ("Correction", "High rigidity"),
}

FUNC_TOKENS = {
    "DEF":           ("Defence", None),
    "AGR":           ("Economic affairs", "Agriculture"),
    "IRR":           ("Economic affairs", "Irrigation"),
    "TRA":           ("Economic affairs", "Transport"),
    "ROA":           ("Economic affairs", "Roads"),
    "RAI":           ("Economic affairs", "Railroads"),
    "WAT_TRA":       ("Economic affairs", "Water transport"),
    "AIR_TRA":       ("Economic affairs", "Air transport"),
    "ENE":           ("Economic affairs", "Energy"),
    "ENV_PRO":       ("Environmental protection", None),
    "R&D_ENV_PRO":   ("Environmental protection", "R&D"),
    "HOU":           ("Housing", None),
    "HEA":           ("Health", None),
    "SEC_EDU":       ("Education", "Secondary education"),
    "EDU":           ("Education", None),
    "SOC_PRO":       ("Social protection", None),
}

REV_TOKENS = {
    "TOT":          ("Total revenue", None),
    "INC_TAX":      ("Income tax", None),
    "COR_INC_TAX":  ("Income tax", "Corporate income tax"),
    "PER_INC_TAX":  ("Income tax", "Personal income tax"),
    "CUS_EXC":      ("Customs and excise", None),
    "CUS":          ("Customs and excise", "Customs"),
    "EXC":          ("Customs and excise", "Excises"),
    "ROY":          ("Other revenue", "Royalties"),
    "PER_FEE":      ("Other revenue", "Permits/Fees"),
    "SOC_CON":      ("Social contributions", None),
}


def longest_match(tokens: list[str], dictionary: dict) -> tuple[tuple, list[str]]:
    """Greedy longest-match scan: try concatenated token windows from longest to
    shortest. Returns (matched_label, remaining_tokens)."""
    n = len(tokens)
    for size in range(min(n, 4), 0, -1):
        for start in range(0, n - size + 1):
            key = "_".join(tokens[start:start + size])
            if key in dictionary:
                return dictionary[key], tokens[:start] + tokens[start + size:]
    return (None, None), tokens


def decompose(code: str) -> dict:
    """Suggested (econ, econ_sub, func, func_sub) from the code string."""
    out = {"suggested_econ": None, "suggested_econ_sub": None,
           "suggested_func": None, "suggested_func_sub": None,
           "tag_kind": None, "decompose_notes": ""}
    parts = code.split("_")
    if not parts or parts[-1] not in ("EXE", "APP"):
        out["decompose_notes"] = "code does not end with _EXE/_APP suffix"
    else:
        parts = parts[:-1]

    if not parts:
        return out
    prefix = parts[0]            # EXP or REV
    if len(parts) < 2:
        out["decompose_notes"] = "code has no classification segment"
        return out
    kind = parts[1]              # ECON / FUNC / CROSS
    body = parts[2:]
    out["tag_kind"] = f"{prefix}_{kind}"

    if prefix == "REV":
        (econ, econ_sub), leftover = longest_match(body, REV_TOKENS)
        out["suggested_econ"] = econ
        out["suggested_econ_sub"] = econ_sub
        if leftover:
            out["decompose_notes"] = f"unmatched REV tokens: {leftover}"
        return out

    if kind == "ECON":
        (econ, econ_sub), leftover = longest_match(body, ECON_TOKENS)
        out["suggested_econ"] = econ
        out["suggested_econ_sub"] = econ_sub
        if leftover:
            out["decompose_notes"] = f"unmatched ECON tokens: {leftover}"
    elif kind == "FUNC":
        (func, func_sub), leftover = longest_match(body, FUNC_TOKENS)
        out["suggested_func"] = func
        out["suggested_func_sub"] = func_sub
        if leftover:
            out["decompose_notes"] = f"unmatched FUNC tokens: {leftover}"
    elif kind == "CROSS":
        # try ECON first then FUNC on the remainder
        (econ, econ_sub), rest = longest_match(body, ECON_TOKENS)
        out["suggested_econ"] = econ
        out["suggested_econ_sub"] = econ_sub
        (func, func_sub), leftover = longest_match(rest, FUNC_TOKENS)
        out["suggested_func"] = func
        out["suggested_func_sub"] = func_sub
        if leftover:
            out["decompose_notes"] = f"unmatched CROSS tokens: {leftover}"
    else:
        out["decompose_notes"] = f"unknown classification kind: {kind}"
    return out


def normalize_criteria_for_dedup(criteria: list[dict]) -> str:
    """Stable string of (field, op, value) excluding the universal year/Exclude filters,
    used to spot duplicate rules."""
    drop = {"Exclude", "year", "year_r"}
    keep = sorted((c["field"], c["op"], c["value"]) for c in criteria if c["field"] not in drop)
    return json.dumps(keep)


def main():
    rows = [r for r in csv.DictReader(open(TAG_RULES))
            if r["row_type"] == "TAG" and r["sheet"] == "Approved"]

    dict_rows = []
    skipped_cross = 0
    for r in rows:
        suggestions = decompose(r["code"])
        if suggestions["tag_kind"] == "EXP_CROSS":
            skipped_cross += 1
            continue
        dict_rows.append({
            "code":             r["code"],
            "category":         r["category"],
            "tag_kind":         suggestions["tag_kind"],
            "econ":             suggestions["suggested_econ"],
            "econ_sub":         suggestions["suggested_econ_sub"],
            "func":             suggestions["suggested_func"],
            "func_sub":         suggestions["suggested_func_sub"],
            "decompose_notes":  suggestions["decompose_notes"],
            "criteria_summary": ", ".join(
                f"{c['field']}{c['op']}{c['value']}"
                for c in json.loads(r["criteria_json"])
                if c["field"] not in ("Exclude", "year", "year_r")
            ),
        })

    with DICT_OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(dict_rows[0].keys()))
        w.writeheader()
        w.writerows(dict_rows)
    print(f"Wrote {len(dict_rows)} rows → {DICT_OUT}")
    print(f"  EXP_CROSS rows skipped (covered by EXP_ECON + EXP_FUNC parents): {skipped_cross}")
    unmatched = [r for r in dict_rows if r["decompose_notes"]]
    print(f"  rows with unmatched tokens: {len(unmatched)}")
    for r in unmatched:
        print(f"    {r['code']}: {r['decompose_notes']}")

    # ---- Suspicious-rule scan for ISSUES.md ----
    suspicious_lines = []

    # 1. duplicate criteria across codes
    by_crit = defaultdict(list)
    for r in rows:
        crits = json.loads(r["criteria_json"])
        by_crit[normalize_criteria_for_dedup(crits)].append(r)
    dups = [v for k, v in by_crit.items() if k != "[]" and len(v) > 1]
    if dups:
        suspicious_lines.append("\n### Duplicate-criteria rules (same filter, different code)\n")
        suspicious_lines.append("Multiple TAG rules resolve to identical SUMIFS criteria — likely a copy-paste oversight or two codes meant to differ via metadata only.\n")
        for group in dups:
            codes = ", ".join(f"`{r['code']}`" for r in group)
            sample = json.loads(group[0]["criteria_json"])
            crit_str = "; ".join(
                f"{c['field']}{c['op']}{c['value']}"
                for c in sample if c["field"] not in ("Exclude", "year", "year_r")
            )
            suspicious_lines.append(f"- {codes} → criteria: `{crit_str or '(no non-universal criteria)'}`")

    # 2. cross-tab where admin1 doesn't match the named ministry in the code
    #    Heuristic: cross-tab code mentions a function token (e.g., HEA, EDU, AGR);
    #    if admin1 criterion's value doesn't contain a related keyword, flag it.
    func_keyword_hint = {
        "HEA": "health", "EDU": "education", "AGR": "agricult",
        "TRA": "transport", "ENE": "energy", "ENV": "environment",
        "HOU": "housing", "DEF": "defence",
    }
    mismatches = []
    for r in rows:
        if not r["code"].startswith("EXP_CROSS_"):
            continue
        crits = json.loads(r["criteria_json"])
        admin1 = next((c for c in crits if c["field"] == "admin1"), None)
        if not admin1:
            continue
        admin_str = (admin1["value"] or "").lower()
        for tok, hint in func_keyword_hint.items():
            if f"_{tok}_" in r["code"] or r["code"].endswith(f"_{tok}_EXE"):
                if hint not in admin_str:
                    mismatches.append((r["code"], tok, admin1["value"]))
                break
    if mismatches:
        suspicious_lines.append("\n### Cross-tabs whose `admin1` does not match the function token in the code\n")
        suspicious_lines.append("Likely a wrong-ministry copy-paste: e.g., 'subsidies in education' filtering by the Ministry of Health.\n")
        for code, tok, admin_val in mismatches:
            suspicious_lines.append(f"- `{code}` mentions `{tok}` but admin1=`{admin_val}`")

    if suspicious_lines:
        # Idempotent: strip any prior §8 before re-appending.
        existing = ISSUES.read_text() if ISSUES.exists() else ""
        marker = "\n## 8. Suspicious tag rules"
        if marker in existing:
            existing = existing.split(marker, 1)[0].rstrip() + "\n"
        new_section = "\n## 8. Suspicious tag rules (auto-detected)\n" + "\n".join(suspicious_lines) + "\n"
        ISSUES.write_text(existing + new_section)
        print(f"\nUpdated ISSUES.md §8 with {len(dups)} duplicate group(s), {len(mismatches)} mismatch(es)")
    else:
        print("\nNo suspicious tag rules detected.")


if __name__ == "__main__":
    main()
