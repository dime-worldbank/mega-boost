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

ROOT = Path("/Users/ysuzuki2/Developer/mega-boost/Moldova/_analysis")
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
DATA.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)
TAG_RULES = DATA / "tag_rules.csv"
DICT_OUT  = DATA / "code_dictionary.csv"
ISSUES    = REPORTS / "ISSUES.md"

# Token dictionaries for Moldova. Labels align with the canonical vocabulary
# shared by the other countries' transform_load notebooks so the outputs union
# cleanly into the cross-country `boost_gold` table. Subnational (SBN_*) and
# the local "rigidity" codes are Moldova-specific and flagged in notes for SME
# review. Multi-token keys win during longest-match scan, so more specific
# tokens like GOO_SER_EMP_CON must be listed ahead of shorter shared prefixes.
ECON_TOKENS = {
    # Wage bill
    "WAG_BIL":             ("Wage bill", None),
    "WAG":                 ("Wage bill", None),
    # Goods and services family
    "GOO_SER_EMP_CON":     ("Goods and services", "Employment contracts"),
    "GOO_SER_BAS_SER":     ("Goods and services", "Basic services"),
    "USE_GOO_SER":         ("Goods and services", None),
    "GOO_SER":             ("Goods and services", None),
    "REC_MAI":             ("Goods and services", "Recurrent maintenance"),
    # Capital
    "CAP_EXP":             ("Capital expenditures", None),
    "CAP_MAI":             ("Capital expenditures", "Capital maintenance"),
    # Transfers & subsidies
    "SUB_PRO":             ("Subsidies", "Subsidies to production"),
    "SUB":                 ("Subsidies", None),
    "CAP_TRA_SOE":         ("Other grants and transfers", "Capital transfers to SOEs"),
    "CUR_TRA_SOE":         ("Other grants and transfers", "Current transfers to SOEs"),
    # Debt
    "INT_DEB":             ("Interest on debt", None),
    # Social
    "SOC_ASS":             ("Social benefits", "Social assistance"),
    "SOC_BEN_PEN":         ("Social benefits", "Pensions"),
    "PEN_CON":             ("Social benefits", "Pension contributions"),
    # Totals / other (no specific econ)
    "TOT_EXP":             (None, None),
    "OTH_MED_RIG_EXP":     ("Other expenses", None),
    # Subnational (Moldova-specific — see SBN_ note emitted below)
    "SBN_CAP_SPE":         ("Capital expenditures", None),
    "SBN_REC_SPE":         ("Goods and services", None),
    "SBN_TOT_SPE":         (None, None),
    "SBN_TOT_TRA":         ("Other grants and transfers", None),
    "SBN_CAP":             ("Capital expenditures", None),
    "SBN_REC":             ("Goods and services", None),
}

FUNC_TOKENS = {
    # Defence / public safety / judiciary / general
    "DEF":                 ("Defence", None),
    "PUB_SAF":             ("Public order and safety", None),
    "JUD":                 ("General public services", "Judiciary"),
    # Economic affairs & sub-sectors
    "ECO_REL":             ("Economic affairs", None),
    "AGR":                 ("Economic affairs", "Agriculture"),
    "IRR":                 ("Economic affairs", "Irrigation"),
    # Energy family (longest-match). ENE is the rollup over all energy
    # sub-sectors; its children (ENE_HEA, ENE_OIL, ENE_POW) live at func_sub.
    "ENE_HEA":             ("Economic affairs", "Energy (heating)"),
    "ENE_OIL_GAS":         ("Economic affairs", "Energy (oil & gas)"),
    "ENE_OIL":             ("Economic affairs", "Energy (oil & gas)"),
    "ENE_POW":             ("Economic affairs", "Energy (power)"),
    "ENE":                 ("Economic affairs", None),
    # Transport family. TRA is the rollup; ROA/RAI/WAT_TRA/TEL are modes.
    "ROA":                 ("Economic affairs", "Roads"),
    "RAI":                 ("Economic affairs", "Railroads"),
    "TEL":                 ("Economic affairs", "Telecom"),
    "WAT_TRA":             ("Economic affairs", "Water transport"),
    "TRA":                 ("Economic affairs", None),
    # Environmental protection. ENV_PRO is the parent rollup; ENV_PRO_NEC is
    # the "not elsewhere classified" sub. Keep the parent at func_sub=None.
    "ENV_PRO_NEC":         ("Environmental protection", "N.E.C"),
    "ENV_PRO":             ("Environmental protection", None),
    # Housing / water supply
    "WAT_SAN":             ("Housing and community amenities", "Water supply"),
    "HOU":                 ("Housing and community amenities", None),
    # Health
    "HEA":                 ("Health", None),
    # Recreation — code mislabeled as REV_CUS_EXC; category confirms COFOG 708.
    "REV_CUS_EXC":         ("Recreation, culture and religion", None),
    # Education (longest-match first)
    "PRI_SEC_EDU":         ("Education", "Primary and secondary education"),
    "PRI_EDU":             ("Education", "Primary education"),
    "SEC_EDU":             ("Education", "Secondary education"),
    "TER_EDU":             ("Education", "Tertiary education"),
    "EDU":                 ("Education", None),
    # Social protection
    "SOC_PRO":             ("Social protection", None),
    # Subnational × function (Moldova-specific)
    "SBN_AGR":             ("Economic affairs", "Agriculture"),
    "SBN_EDU":             ("Education", None),
    "SBN_HEA":             ("Health", None),
}

REV_TOKENS = {
    "TOT":                 ("Total revenue", None),
    # Income tax
    "COR_INC_TAX":         ("Income tax", "Corporate income tax"),
    "PER_INC_TAX":         ("Income tax", "Personal income tax"),
    "INC_TAX":             ("Income tax", None),
    # VAT / excises / customs. CUS and EXC are rollups (customs total, excises
    # total); their specific sub-codes (CUS_IMP/EXP, EXC_FUE/TOB/ALC) live at
    # econ_sub. CUS_EXC is the combined customs+excise rollup.
    "VAT":                 ("Taxes on goods and services", "VAT"),
    "CUS_EXC":             ("Customs and excise", None),
    "CUS_IMP":             ("Customs and excise", "Customs (imports)"),
    "CUS_EXP":             ("Customs and excise", "Customs (exports)"),
    "CUS":                 ("Customs and excise", None),
    "EXC_FUE":             ("Customs and excise", "Excises (fuels)"),
    "EXC_TOB":             ("Customs and excise", "Excises (tobacco)"),
    "EXC_ALC":             ("Customs and excise", "Excises (alcohol)"),
    "EXC":                 ("Customs and excise", None),
    # Property
    "PRO_TAX_IMM":         ("Property taxes", "Immovable"),
    "PRO_TAX_TRA":         ("Property taxes", "Transactions"),
    "PRO_TAX":             ("Property taxes", None),
    # Social contributions
    "SOC_CON_ERS":         ("Social contributions", "Employer"),
    "SOC_CON_EES":         ("Social contributions", "Employee"),
    "SOC_CON":             ("Social contributions", None),
    # Other
    "PER_FEE":             ("Other revenue", "Permits/Fees"),
    "ROY":                 ("Other revenue", "Royalties"),
    # Subnational (Moldova-specific — see SBN_ note emitted below)
    "SBN_OWN":             (None, None),
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

    # Moldova-specific flags for SME review.
    notes = [out["decompose_notes"]] if out["decompose_notes"] else []
    if "SBN_" in code:
        notes.append("Moldova subnational tag — confirm admin0/geo0 handling with SME")
    if "REV_CUS_EXC" in code and kind == "FUNC":
        notes.append(
            "code token REV_CUS_EXC appears mislabeled: the workbook's category "
            "is 'Recreation, culture and religion (COFOG 708)' — rename the code "
            "upstream or treat as COFOG 708"
        )
    out["decompose_notes"] = "; ".join(notes)
    return out


def normalize_criteria_for_dedup(criteria: list[dict]) -> str:
    """Stable string of (field, op, value) excluding the universal year/Exclude filters,
    used to spot duplicate rules."""
    drop = {"Exclude", "year", "year_r"}
    keep = sorted((c["field"], c["op"], c["value"]) for c in criteria if c["field"] not in drop)
    return json.dumps(keep)


# Meta-rollup codes that span an entire COFOG or econ category — they
# definitionally contain their component codes (e.g. ECO_REL = all COFOG
# 704 → includes ENE, TRA, AGR, ROA …). Excluded for the same reason as
# EXP_CROSS_*: they would double-count against their components in the
# cross-country aggregate and clutter overlap detection with parent
# rollups that are expected to intersect their children.
META_ROLLUP_CODES = {
    "EXP_FUNC_ECO_REL_EXE",
}


def main():
    # Use TAG rows from both Approved and Executed. Moldova has a handful of
    # codes that only have formulas in Executed (e.g., EXP_ECON_CAP_MAI_EXE,
    # EXP_FUNC_IRR_EXE) — the dictionary needs all of them.
    rows = [r for r in csv.DictReader(open(TAG_RULES))
            if r["row_type"] == "TAG"]
    rows.sort(key=lambda r: (r["code"], 0 if r["sheet"] == "Approved" else 1))

    # tag_rules.csv can carry multiple rows per code (one per year-range shape).
    # The dictionary is code-level — dedupe keeping the first occurrence so each
    # code produces exactly one row.
    seen_codes: set[str] = set()
    dict_rows = []
    skipped_cross = 0
    skipped_meta = 0
    for r in rows:
        if r["code"] in seen_codes:
            continue
        seen_codes.add(r["code"])
        if r["code"] in META_ROLLUP_CODES:
            skipped_meta += 1
            continue
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
    print(f"  Meta-rollup codes skipped ({', '.join(sorted(META_ROLLUP_CODES))}): {skipped_meta}")
    unmatched = [r for r in dict_rows if r["decompose_notes"]]
    print(f"  rows with unmatched tokens: {len(unmatched)}")
    for r in unmatched:
        print(f"    {r['code']}: {r['decompose_notes']}")

    # ---- reports/code_hierarchy.md : human-readable tree for SME review ----
    hier_path = REPORTS / "code_hierarchy.md"
    hier = [
        "# Moldova BOOST — code-dictionary hierarchy for SME review\n",
        "Every non-CROSS TAG code mapped to canonical `econ` / `econ_sub` / "
        "`func` / `func_sub` labels shared with the other countries in the "
        "cross-country aggregate. Review the groupings below and flag anything "
        "that should land under a different parent, plus any code flagged in "
        "the _Notes_ column.\n",
        f"- Non-CROSS TAG codes in dictionary: **{len(dict_rows)}**",
        f"- `EXP_CROSS_*` codes excluded (double-count with EXP_ECON×EXP_FUNC parents): **{skipped_cross}**",
        f"- Meta-rollup codes excluded ({', '.join(sorted(META_ROLLUP_CODES))}): **{skipped_meta}**",
        f"- Codes with a flagged `decompose_notes` entry: **{len(unmatched)}**\n",
    ]

    def _code_line(r):
        note = f" _⚠ {r['decompose_notes']}_" if r["decompose_notes"] else ""
        cat = r["category"] or ""
        return f"  - `{r['code']}` — {cat}{note}"

    def _render_tree(rows: list[dict], level_a: str, level_b: str, title: str):
        hier.append(f"\n## {title}\n")
        bucket: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
        orphans: list[dict] = []
        for r in rows:
            a = r[level_a] or ""
            b = r[level_b] or ""
            if not a and not b:
                orphans.append(r)
            else:
                bucket[a or "(unmapped)"][b].append(r)
        for a in sorted(bucket):
            total = sum(len(v) for v in bucket[a].values())
            hier.append(f"### {a or '(unmapped)'} — {total} code(s)")
            # top-level codes (b empty) first, then sub-groups
            if "" in bucket[a]:
                for r in sorted(bucket[a][""], key=lambda x: x["code"]):
                    hier.append(_code_line(r))
            for b in sorted(k for k in bucket[a] if k):
                hier.append(f"  - **{b}**")
                for r in sorted(bucket[a][b], key=lambda x: x["code"]):
                    hier.append(f"  " + _code_line(r))
            hier.append("")
        if orphans:
            hier.append("### (no assignment yet)")
            for r in sorted(orphans, key=lambda x: x["code"]):
                hier.append(_code_line(r))
            hier.append("")

    _render_tree(
        [r for r in dict_rows if r["tag_kind"] == "EXP_ECON"],
        "econ", "econ_sub", "Expenditure — Economic classification (EXP_ECON)",
    )
    _render_tree(
        [r for r in dict_rows if r["tag_kind"] == "EXP_FUNC"],
        "func", "func_sub", "Expenditure — Functional classification (EXP_FUNC)",
    )
    _render_tree(
        [r for r in dict_rows if r["tag_kind"] == "REV_ECON"],
        "econ", "econ_sub", "Revenue — Economic classification (REV_ECON)",
    )

    hier.append(
        "\n## Review questions for the SME\n\n"
        "1. Any code parked under the wrong `econ` / `func` parent?\n"
        "2. For codes carrying `⚠` notes (subnational `SBN_*`, mislabeled "
        "`REV_CUS_EXC`, any unmatched tokens): decide canonical treatment.\n"
        "3. `EXP_CROSS_*` codes are excluded to avoid double-counting against "
        "the `EXP_ECON` × `EXP_FUNC` parents. If the SME wants any specific "
        "CROSS kept as a primary dimension, call it out.\n"
    )
    hier_path.write_text("\n".join(hier))
    print(f"Wrote {hier_path}")

    # ---- Suspicious-rule scan for ISSUES.md ----
    suspicious_lines = []

    # 1. duplicate criteria across distinct codes
    # Same code may appear multiple times (per year-range × Approved/Executed);
    # group by criteria but flag only buckets containing 2+ *distinct* codes.
    by_crit = defaultdict(list)
    for r in rows:
        crits = json.loads(r["criteria_json"])
        by_crit[normalize_criteria_for_dedup(crits)].append(r)
    dups = []
    for k, group in by_crit.items():
        if k == "[]":
            continue
        distinct_codes = {r["code"] for r in group}
        if len(distinct_codes) > 1:
            # keep one row per distinct code for rendering
            first_per_code = {}
            for r in group:
                first_per_code.setdefault(r["code"], r)
            dups.append(list(first_per_code.values()))
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
        # Moldova's SBN_* codes deliberately filter by admin1="locale"/"local"
        # to select subnational rows — that's not a copy-paste mismatch.
        if "_SBN_" in r["code"]:
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
        # Idempotent: strip any prior §6 (or older §8) before re-appending.
        existing = ISSUES.read_text() if ISSUES.exists() else ""
        for marker in ("\n## 6. Suspicious tag rules", "\n## 8. Suspicious tag rules"):
            if marker in existing:
                existing = existing.split(marker, 1)[0].rstrip() + "\n"
                break
        new_section = "\n## 6. Suspicious tag rules (auto-detected)\n" + "\n".join(suspicious_lines) + "\n"
        ISSUES.write_text(existing + new_section)
        print(f"\nUpdated ISSUES.md §6 with {len(dups)} duplicate group(s), {len(mismatches)} mismatch(es)")
    else:
        print("\nNo suspicious tag rules detected.")


if __name__ == "__main__":
    main()
