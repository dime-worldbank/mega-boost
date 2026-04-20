"""Build a single SME-facing verification document for Moldova BOOST.

Scope (kept intentionally small — this is a demo handoff, not a full audit):
  A. Formula overcounting — pairs of codes whose SUMIFS filters tag the
     same raw rows. Cross-category pairs = bugs; same-value pairs = dupes.
  B. Hard-coded overrides — cells where a value was typed directly
     instead of a SUMIFS formula; the pipeline reproduces SUMIFS only.
  C. Sign-off checklist — one checkbox per item A + B so the SME can
     walk the doc top-to-bottom and confirm each proposed fix.

Each proposed fix follows the cross-country pattern established in
`temp/Questions for Massimo.docx` (more-specific SUMIFS wins; rename/drop
one of a duplicate pair; recover an override as an explicit formula).

Inputs:
  Moldova/_onboarding/data/overlap_detail.csv
  Moldova/_onboarding/data/hardcoded_cells.csv
  Moldova/tag_rules.csv

Output:
  Moldova/_onboarding/reports/VERIFICATION.md
"""
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # Moldova/_onboarding
COUNTRY_DIR = ROOT.parent                              # Moldova/
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
OVERLAP_CSV = DATA / "overlap_detail.csv"
HARDCODED_CSV = DATA / "hardcoded_cells.csv"
TAG_RULES = COUNTRY_DIR / "tag_rules.csv"
OUT = REPORTS / "VERIFICATION.md"


# ---------------------------------------------------------------------------
# Cross-country category-priority precedents from previous fixes applied to
# other countries. Higher row = higher priority. When two codes overlap, the
# lower-priority code must exclude rows already claimed by the higher-priority
# code.
#
#   Wage bill  >  Social assistance        (KEN)
#   Goods & services  >  Social assistance (KEN)
#   Capital expenditures  >  Social assistance  (KEN)
#   Capital expenditures  >  Subsidies     (BTN — Massimo explicit)
#   Wage bill  >  Subsidies                (TUN — Massimo explicit)
#   Goods & services  >  Subsidies         (TUN — Massimo explicit)
#   Subsidies  >  Social assistance        (KEN — via `<>25*` exclusion)
#   Public safety  >  Defence              (TUN)
#
# Rollup / duplicate-label cases are NOT priority decisions — they are
# taxonomy bugs: drop the derived-total code, or correct the mislabelled
# formula.
# ---------------------------------------------------------------------------

OVERLAP_FIX = {
    frozenset({"EXP_ECON_GOO_SER_EMP_CON_EXE", "EXP_ECON_SOC_ASS_EXE"}): {
        "kind": "priority",
        "winner": "EXP_ECON_GOO_SER_EMP_CON_EXE",
        "loser": "EXP_ECON_SOC_ASS_EXE",
        "diagnosis": (
            "`EXP_ECON_GOO_SER_EMP_CON_EXE` is a Goods & services sub-"
            "category (employment contracts). Based on the Kenya fix, "
            "Goods & services takes precedence over Social assistance. "
            "Rows currently counted in both must be assigned only to G&S."
        ),
        "fix": (
            "Add `econ2,\"<>113.16*\"` to Social assistance so G&S "
            "(employment contracts) rows are excluded."
        ),
    },
    frozenset({"EXP_ECON_REC_MAI_EXE", "EXP_ECON_SOC_ASS_EXE"}): {
        "kind": "priority",
        "winner": "EXP_ECON_REC_MAI_EXE",
        "loser": "EXP_ECON_SOC_ASS_EXE",
        "diagnosis": (
            "`EXP_ECON_REC_MAI_EXE` is a Goods & services sub-category "
            "(recurrent maintenance). Based on the Kenya fix, Goods & "
            "services takes precedence over Social assistance."
        ),
        "fix": (
            "Add `econ2,\"<>113.18*\"` to Social assistance so recurrent-"
            "maintenance rows are excluded."
        ),
    },
    frozenset({"EXP_ECON_SOC_ASS_EXE", "EXP_ECON_SUB_PRO_EXE"}): {
        "kind": "priority",
        "winner": "EXP_ECON_SUB_PRO_EXE",
        "loser": "EXP_ECON_SOC_ASS_EXE",
        "diagnosis": (
            "`EXP_ECON_SUB_PRO_EXE` is a Subsidies sub-category (subsidies "
            "to production). Based on the Kenya fix, Subsidies takes "
            "precedence over Social assistance."
        ),
        "fix": (
            "Add `econ1,\"<>132*\"` to Social assistance so subsidies-to-"
            "production rows are excluded."
        ),
    },
    frozenset({"EXP_FUNC_PRI_EDU_EXE", "EXP_FUNC_PRI_SEC_EDU_EXE"}): {
        "kind": "rollup",
        "diagnosis": (
            "`EXP_FUNC_PRI_SEC_EDU_EXE` is a **rollup** (Preschool + "
            "Primary + Secondary). `EXP_FUNC_PRI_EDU_EXE` (Preschool + "
            "Primary) is wholly inside it, so 100% of PRI is double-counted "
            "inside PRI_SEC. This is a taxonomy bug, not a priority call."
        ),
        "fix": (
            "Drop `EXP_FUNC_PRI_SEC_EDU_EXE` as a reported func_sub — it is "
            "a derived total, not a leaf. Keep `EXP_FUNC_PRI_EDU_EXE` and "
            "`EXP_FUNC_SEC_EDU_EXE` as the disjoint leaves."
        ),
    },
    frozenset({"EXP_FUNC_PRI_SEC_EDU_EXE", "EXP_FUNC_SEC_EDU_EXE"}): {
        "kind": "rollup",
        "diagnosis": (
            "Same rollup issue: `EXP_FUNC_PRI_SEC_EDU_EXE` contains "
            "`EXP_FUNC_SEC_EDU_EXE` in full (100% overlap)."
        ),
        "fix": (
            "Same action: drop `EXP_FUNC_PRI_SEC_EDU_EXE`."
        ),
    },
    frozenset({"REV_ECON_CUS_EXC_EXE", "REV_ECON_EXC_EXE"}): {
        "kind": "duplicate",
        "diagnosis": (
            "`REV_ECON_CUS_EXC_EXE` and `REV_ECON_EXC_EXE` resolve to "
            "**identical** SUMIFS (both filter `econ4=114200 Accize`). The "
            "label _Customs/excise_ implies Customs should also be "
            "included; the current formula only captures Excises."
        ),
        "fix": (
            "Option 1 — fix `REV_ECON_CUS_EXC_EXE` to include customs:\n"
            "`=SUMIFS(approved_NN,year_NN,X$1,transfer_NN,"
            "\"Cu exceptia transferurilor\",econ0_NN,\"Revenues\","
            "econ4_NN,{\"114200 Accize\",\"114100 Taxe vamale\"})`\n\n"
            "Option 2 — drop one of the two codes as redundant."
        ),
    },
}


def _pair_key(a: str, b: str) -> frozenset:
    return frozenset({a, b})


# ---------------------------------------------------------------------------
# Severity classifier. Uses the larger of (approved%, executed%) of the
# overcounted amount relative to the smaller code's total — the same
# denominator used in overlap_report.md.
# ---------------------------------------------------------------------------
def severity(ratio: float) -> str:
    if ratio >= 0.50:
        return "Critical"
    if ratio >= 0.10:
        return "High"
    if ratio >= 0.01:
        return "Medium"
    return "Low"


def _fmt_money(x: float) -> str:
    return f"{x:,.0f}" if x is not None else ""


# ---------------------------------------------------------------------------
# Load overlap detail and aggregate per (code_a, code_b, range).
# ---------------------------------------------------------------------------
def load_overlaps():
    pairs = defaultdict(lambda: {
        "shared": "", "cross": False, "rows": 0,
        "approved_over": 0.0, "executed_over": 0.0,
        "per_year": [],
    })
    with open(OVERLAP_CSV, newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            key = (r["code_a"], r["code_b"], r["range"])
            d = pairs[key]
            d["shared"] = r["shared"]
            d["cross"] = r["cross_category"] == "True"
            d["rows"] += int(float(r["rows_overlap"]))
            d["approved_over"] += float(r["approved_overcounted"] or 0)
            d["executed_over"] += float(r["executed_overcounted"] or 0)
            d["per_year"].append({
                "year": int(float(r["year"])),
                "rows": int(float(r["rows_overlap"])),
                "approved": float(r["approved_overcounted"] or 0),
                "executed": float(r["executed_overcounted"] or 0),
            })
    return pairs


# ---------------------------------------------------------------------------
# Pull each code's full-range totals from parsing_verification.csv so we can
# express overcounted amounts as % of the smaller code's own total.
# ---------------------------------------------------------------------------
RANGE_LABEL = {"base": "2006-2015", "16": "2016-2019", "20": "2020-2024"}


def load_code_totals():
    """totals[(code, range_label)][measure] = Σ excel cached values."""
    totals = defaultdict(lambda: defaultdict(float))
    pv = DATA / "parsing_verification.csv"
    with open(pv, newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            code = r["code"]
            rng_label = RANGE_LABEL.get(r["range"], r["range"])
            m = "approved" if r["measure"].startswith("approved") else "executed"
            try:
                totals[(code, rng_label)][m] += float(r["excel"] or 0)
            except ValueError:
                pass
    return totals


# ---------------------------------------------------------------------------
# Hardcoded overrides.
# ---------------------------------------------------------------------------
RANGE_TO_RULE = {"2006-2015": "base", "2016-2019": "16", "2020-2024": "20"}


def load_sample_formulas():
    """formulas[(code, range_label)] = one representative sample formula.

    A code may have multiple rows per range (one per shape-group). We take the
    Executed-measure sample when available — it tends to be the fully-expanded
    form including `-Cn` cell subtractions and Raw2 supplements.
    """
    formulas = {}
    with open(TAG_RULES, newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            code = r["code"]
            rng_key = r.get("sample_year_range", "")
            # Fall back to inferring the range from the measure suffix
            measure = r.get("measure", "")
            if measure.endswith("_16"):
                rng_label = "2016-2019"
            elif measure.endswith("_20"):
                rng_label = "2020-2024"
            else:
                rng_label = "2006-2015"
            key = (code, rng_label)
            # Prefer executed sample; keep first seen if already stored.
            if key not in formulas or measure.startswith("executed"):
                formulas[key] = r.get("sample_formula", "").strip()
    return formulas


def load_hardcoded():
    rows = []
    with open(HARDCODED_CSV, newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append(r)
    return rows


def build_overlap_section(pairs, totals, formulas):
    lines = []
    lines.append("## A. Formula overcounting")
    lines.append("")
    lines.append(
        "Rows tagged by multiple codes that are meant to be mutually exclusive. "
        "The _Overcounted_ column is the sum of the raw measure (approved / "
        "executed) over rows that match **both** codes' SUMIFS filters — the "
        "exact amount that is double-counted when the two codes are summed "
        "together. Severity is `Overcounted ÷ min(Σ code A, Σ code B)` — a 100% "
        "figure means one code's total is wholly contained in the other's."
    )
    lines.append("")

    # Group by range label
    by_range = defaultdict(list)
    for (a, b, rng), d in pairs.items():
        by_range[rng].append((a, b, d))

    range_order = ["2006-2015", "2016-2019", "2020-2024"]
    idx = 0
    for rng in range_order:
        if rng not in by_range:
            continue
        lines.append(f"### Range {rng}")
        lines.append("")
        for a, b, d in sorted(by_range[rng]):
            idx += 1
            ta = totals.get((a, rng), {})
            tb = totals.get((b, rng), {})
            # Compute severity ratio using approved (fallback executed) smaller denominator.
            def _ratio(measure):
                over = d[f"{measure}_over"]
                denom = min(ta.get(measure, 0) or 0, tb.get(measure, 0) or 0)
                return (over / denom) if denom > 0 else 0.0
            ratio = max(_ratio("approved"), _ratio("executed"))
            sev = severity(ratio)
            kind = "cross-category" if d["cross"] else "same-value duplicate"

            meta = OVERLAP_FIX.get(_pair_key(a, b))
            if meta is None:
                meta = {
                    "kind": "unknown",
                    "diagnosis": "_No canned guidance — SME to propose._",
                    "fix": "_Awaiting SME direction._",
                }

            lines.append(f"#### A{idx}. `{a}` × `{b}` — **{sev}** ({kind})")
            lines.append("")
            lines.append(f"- Shared level: `{d['shared']}`")
            lines.append(
                f"- Intersection: {d['rows']:,} raw rows · "
                f"Σ approved **{_fmt_money(d['approved_over'])}** · "
                f"Σ executed **{_fmt_money(d['executed_over'])}**"
            )
            lines.append(
                f"- Severity ratio (overcounted ÷ min total): "
                f"approved {_ratio('approved') * 100:.1f}% · "
                f"executed {_ratio('executed') * 100:.1f}%"
            )
            lines.append("")
            lines.append("**Current Excel formulas.**")
            lines.append("")
            for code in (a, b):
                fml = formulas.get((code, rng), "_(not captured)_")
                lines.append(f"- `{code}`:")
                lines.append(f"  ```")
                lines.append(f"  {fml}")
                lines.append(f"  ```")
            lines.append("")
            if meta["kind"] == "priority":
                lines.append(
                    f"**Priority (based on previous fixes).** "
                    f"`{meta['winner']}` **>** `{meta['loser']}` — overlapping "
                    f"rows are assigned to the higher-priority code."
                )
                lines.append("")
            lines.append(f"**Diagnosis.** {meta['diagnosis']}")
            lines.append("")
            lines.append("**Proposed fix.**")
            lines.append("")
            lines.append(meta["fix"])
            lines.append("")
    return lines


def build_hardcoded_section(rows):
    lines = []
    lines.append("## B. Hard-coded overrides")
    lines.append("")
    lines.append(
        f"**{len(rows)} cells** in Approved / Executed contain a typed numeric "
        "literal instead of a SUMIFS formula. The pipeline reproduces SUMIFS "
        "results only, so each override below will surface as a per-run "
        "discrepancy. Proposed fix (cross-country pattern): recover each "
        "override as an explicit formula — usually a SUMIFS minus a specific "
        "sibling row, or a SUMIFS over a different filter — so the calculation "
        "is reproducible and auditable."
    )
    lines.append("")

    # Group by code → rows
    by_code = defaultdict(list)
    for r in rows:
        by_code[r["code"]].append(r)

    idx = 0
    for code, items in sorted(by_code.items()):
        idx += 1
        cat = items[0]["category"]
        years = sorted({int(i["header"]) for i in items})
        sheets = sorted({i["sheet"] for i in items})
        lines.append(f"### B{idx}. `{code}` — {cat}")
        lines.append("")
        lines.append(
            f"- Sheets: {', '.join(sheets)} · Years: {min(years)}–{max(years)} "
            f"· {len(items)} cells total"
        )
        lines.append("")
        lines.append("| Sheet | Cell | Year | Value |")
        lines.append("|---|---|---:|---:|")
        for i in sorted(items, key=lambda x: (x["sheet"], int(x["header"]))):
            lines.append(
                f"| {i['sheet']} | {i['cell']} | {i['header']} | "
                f"{int(i['value']):,} |"
            )
        lines.append("")
        lines.append(
            "**Proposed fix.** Ask the SME to supply the SUMIFS expression "
            "(or SUMIFS − sibling-cell difference) that produced each value, "
            "then replace the literal in the workbook. Once the Excel is "
            "reformulated, the pipeline will reproduce it automatically."
        )
        lines.append("")
    return lines


def build_signoff_section(pairs, rows):
    lines = []
    lines.append("## C. Sign-off checklist")
    lines.append("")
    lines.append(
        "One checkbox per item. Tick _Accept_ if the proposed fix should be "
        "applied to the workbook; tick _Reject_ with a note if the current "
        "behaviour is correct as-is."
    )
    lines.append("")
    lines.append("### A. Overcounting")
    lines.append("")
    idx = 0
    for rng in ["2006-2015", "2016-2019", "2020-2024"]:
        for (a, b, r), _ in sorted(pairs.items()):
            if r != rng:
                continue
            idx += 1
            lines.append(f"- [ ] **A{idx}** `{a}` × `{b}` ({rng}) — accept fix / reject (note: _______)")
    lines.append("")
    lines.append("### B. Hard-coded overrides")
    lines.append("")
    by_code = defaultdict(int)
    for r in rows:
        by_code[r["code"]] += 1
    idx = 0
    for code, count in sorted(by_code.items()):
        idx += 1
        lines.append(f"- [ ] **B{idx}** `{code}` ({count} cells) — SME to supply formulas / accept literals")
    lines.append("")
    lines.append("### Final")
    lines.append("")
    lines.append("- [ ] Reviewer: __________________________")
    lines.append("- [ ] Date: __________________________")
    lines.append(
        "- [ ] Workbook updated; rerun `python3 _onboarding/scripts/6_detect_overlaps.py` "
        "and `python3 _onboarding/scripts/9_build_verification_doc.py` to "
        "confirm resolved items drop out."
    )
    return lines


def main():
    pairs = load_overlaps()
    totals = load_code_totals()
    formulas = load_sample_formulas()
    hardcoded = load_hardcoded()

    out = []
    out.append("# Moldova BOOST — Expert Verification")
    out.append("")
    out.append(
        "This document lists every item in the Moldova BOOST workbook that "
        "needs subject-matter-expert confirmation before we publish the "
        "pipeline output. Two classes of issues are covered:"
    )
    out.append("")
    out.append(
        "1. **Formula overcounting** — rows double-counted by multiple "
        "codes that are meant to be mutually exclusive."
    )
    out.append(
        "2. **Hard-coded overrides** — cells where a literal value replaces "
        "the formula, so the pipeline can't reproduce it."
    )
    out.append("")
    out.append(
        "Each item shows the current Excel behaviour, the severity, and a "
        "proposed fix based on previous fixes applied to other countries "
        "(category priority for overlapping codes; rename or drop duplicate / "
        "rollup codes; recover hard-coded overrides as explicit formulas). "
        "The sign-off checklist at the bottom captures accept / reject per "
        "item — once signed, we update the workbook and the pipeline reruns "
        "clean."
    )
    out.append("")

    # Executive summary counts
    n_cross = sum(1 for (_, _, _), d in pairs.items() if d["cross"])
    n_same = sum(1 for (_, _, _), d in pairs.items() if not d["cross"])
    n_overrides = len(hardcoded)
    n_override_codes = len({r["code"] for r in hardcoded})
    out.append("**At a glance**")
    out.append("")
    out.append(f"- Overcounting pairs: **{len(pairs)}** "
               f"({n_cross} cross-category, {n_same} same-value duplicate)")
    out.append(f"- Hard-coded overrides: **{n_overrides}** cells across "
               f"{n_override_codes} code(s)")
    out.append("")

    out += build_overlap_section(pairs, totals, formulas)
    out += build_hardcoded_section(hardcoded)
    out += build_signoff_section(pairs, hardcoded)

    OUT.write_text("\n".join(out) + "\n")
    print(f"Wrote {OUT} ({len(out)} lines)")


if __name__ == "__main__":
    main()
