---
name: boost-overcounting
description: Onboard or audit a country's BOOST workbook for expenditure overcounting. Use when adding a new country to mega-boost, or when the Executed-sheet SUMIFS categories double-count budget lines — to build the formula→microdata detector, validate every (code,year) cell against its cached value, find WITHIN-dimension overlaps (econ / econ_sub / func), flag within-formula self-doubles and Excel array-broadcast bugs, and produce a neutral pairwise verification.md of corrected formulas for expert sign-off. Triggers include "onboard <country>", "overcounting", "double counting", "BOOST verification", "detect overlaps", "reconcile categories".
---

# BOOST country onboarding — overcounting detection & verification

Worked reference: the **Uganda** folder — `Uganda/UGA_detect_overcounting.py`, `Uganda/verification.md`, `Uganda/_detect/`. Copy and adapt; this skill is the method + the hard-won gotchas.

## Deliverables (in `<Country>/`)
1. **`<XXX>_extract_microdata_excel_to_csv.py`** — extracts the workbook's microdata to CSV; the input every later step reads.
2. **`<XXX>_transform_load_dlt.py`** — the per-line pipeline implementing the agreed disjoint classification (extend the existing one).
3. **`verification.md`** — the review/sign-off doc: every overlap as a pair, both categories' formulas, magnitude, proposed fix under a stated default, decisions for the expert. (Optional `verification.docx`, see appendix.)

**Temp / diagnostic (NOT a shipped deliverable):** `<XXX>_detect_overcounting.py` — the standalone pandas detector (formula→microdata validator + overlap finder) you build to *produce* the verification.md findings. Copy Uganda's; only the country-specific maps change. Throwaway once the workbook formulas + transform are agreed.

## Inputs
- Workbook `temp/<Country> BOOST.xlsx`. The **`Executed` sheet** holds, per code (rows) × year (cols), a `SUMIFS` formula + a cached value; defined names map the SUMIFS named ranges → microdata columns.
- The microdata CSV (output of `<XXX>_extract_microdata_excel_to_csv.py`) — one row per budget line.

> **Layout varies by country — confirm, don't assume.** The Executed sheet is **not always `xl/worksheets/sheet2.xml`**: resolve it by name via `xl/workbook.xml` (sheet name → `r:id`) + `xl/_rels/workbook.xml.rels` (`r:id` → target). The **year-column start** (C, D, …), the **code prefix** (`EXP_*` is typical but check), and which **rows hold the code/label/year-header** can all differ. Verify against the actual workbook before trusting `read_executed_sheet`.

## Step 1 — Detector *(temp diagnostic — reuse the engine; swap the maps)*
Keep the engine logic: `_shift_formula`, `_residual_signed_refs`, `parse_terms`, `_cond_mask`, `term_mask`, `code_value_and_mask`. Per country, confirm/adjust:
- **`read_executed_sheet` constants** — the Executed-sheet XML path (resolve by name, *not* hardcoded `sheet2.xml`), the year-column range, and the column-A code-prefix test (`startswith("EXP")` — check the prefix). These vary by workbook (see Inputs).
- `NAMED_RANGE_TO_HEADER` — workbook named-range → CSV header (read the workbook's defined names).
- `YEAR_COLS` — year columns present.
- `DIMENSIONS` = `ECON_TOP` / `ECON_SUB` / `FUNC_TOP` — display name → the leaf `EXP_*` code(s) whose masks define each category. **Use the genuine top-level code** (e.g. the COFOG-706 Housing code), never a proxy/leaf as a stand-in.
- `EXCEL_FORMULA_ERRORS` — fill in as you confirm workbook bugs.

### Faithful-parsing checklist — every one of these bit during Uganda
- **Shared formulas (biggest trap).** Excel writes a row's formula once on the master cell (`<f t="shared" ref="C..:U.." si="N">…</f>`); the other cells are `<f t="shared" si="N"/>` with **no text**. Resolve each slave to its master, **shifting relative A1 refs by the column offset**. Skip this and most cells look formula-less (Uganda: validated cells jumped 1216 → 3090 once fixed).
- **Literal text matching.** Excel trims **neither** cells nor criteria — trailing/leading spaces are significant on **both** sides. Do NOT `.str.strip()` the column. Real criteria include `func0 "… Housing "` and `<>"0600 Unspecified "` (the latter's trailing space is a typo that matches nothing — flag it).
- **Composite cell-refs.** Some cells are `SUMIFS(…) + Cnnn` or `… − Cnnn` (e.g. Total − debt; Housing = lands + the water&san cell). Resolve `±CELLREF` recursively into the referenced code's terms.
- **Year-varying criteria.** Criteria change at the country's recode era (Uganda: FY2022/23 — sector names → renumbered `func0` + per-line flags). Parse each year-column's **own** formula.

## Step 2 — Validate (formula→microdata vs cached). Classify every (code,year):
- **match** — reproduces within tolerance (target: all non-buggy cells).
- **excel_formula_error** — the workbook formula is arithmetically wrong, python is right. Pattern seen: **array-broadcast double-count** — `SUM( SUMIFS(…array criteria…) + SUM(SUMIFS(…)) )`: the inner scalar is broadcast across the array, doubling it. Fix: wrap **each** SUMIFS in its own `SUM()`. Record in `EXCEL_FORMULA_ERRORS`.
- **parser_mismatch** — your bug; fix the parser/maps first. Drive to 0.

## Step 3 — Detect overlaps (WITHIN one dimension only)
- **Dimensions are orthogonal.** A line legitimately carries one `econ` tag AND one `func` tag — econ×func overlap is **expected, not a problem**. Only flag a line counted by >1 category **within the same dimension**.
- Run on **econ, econ_sub, func**. **Defer func_sub** until the func/func_sub COFOG hierarchy (Transport⊃Roads, Energy⊃Power, …) is agreed with the experts — reporting it prejudges that hierarchy.
- Also run the **within-formula self-double** check: a line matched by >1 **additive** term of the *same* code's `SUM(SUMIFS)+SUM(SUMIFS)` is counted twice inside one category. **Validation cannot see this** (Excel and python sum the terms the same inflated way). Uganda: Capital exp 2.32T (its `econ2 "31"/"23"` terms ∩ its own `add,"capital"` term).

## Step 4 — Transform (per-line `.when()`, order-proof)
- Each category = a mutually-exclusive predicate, **disjoint by construction**; reordering `.when()` branches changes nothing. Assert `n_econ = 1`, `n_func = 1`.
- **OR semantics within a category** (a line matched by two of a category's terms counts once) — this is why the pipeline does NOT self-double even where the Excel `+` formula does.
- **Year-aware** (branch at the era cutover). Resolve each overlap by an explicit discriminating criterion, not "first match wins".

## Step 5 — verification.md (NEUTRAL — the expert decides precedence)
Hardest-won shape:
- **Organize by pair.** One row per overlapping pair, per dimension.
- **Show both categories' current formulas**, each cell explicitly labelled `A = <Category>` / `B = <Category>`.
- Columns: `Overlap pair · size | Category A (formula) | Category B (formula) | Shared lines | Proposed fix (under default)`.
- **Do NOT assume which side is over-counted.** The proposed fix is shown *under a suggested default*; the expert decides which category owns the shared lines and the exclusion then sits on the **other** side (or another split). Make defaults trivially flippable.
- **Separate sections (do not force into the pair table):** (a) within-formula self-double — one category, two terms; (b) confirmed Excel arithmetic bugs — the cell is simply wrong, no precedence to decide.
- **Decisions table = the control panel:** a few precedence knobs (one decision can govern many pairs — e.g. "pensions = COFOG 710" flips every `… ∩ Social protection` pair) **plus** the non-pairwise questions (total definition, coverage gaps, where excluded lines land). Per-pair fixes are the derived consequence.
- Compact conventions: state once that every formula is `SUMIFS(executed, year, <col>$1, …)` and show **criteria only**; define shorthands (`bt = budget_type,"<>…"`). Drop prose that needs no expert review.

## Principles (don't relearn)
- Overlaps matter only WITHIN a dimension; econ and func are independent.
- Measure every magnitude on the microdata — never assert.
- Distinguish the THREE failure types and keep them in separate sections: **(a)** cross-category overlap → precedence decision; **(b)** within-formula self-double → formula bug, no precedence; **(c)** Excel arithmetic bug → cell wrong.
- The pipeline can be correct while the workbook double-counts (OR vs `+`). Fix the workbook formula; the pipeline already counts once.
- Precedence is the **expert's** call. Stay neutral; suggest a default; make it flippable.

## Appendix — verification.docx
`python-docx` (pip install). Convert with: landscape, 0.5" margins, table font ~7pt (the formula tables are wide); render `**bold**` as bold, `` `code` `` as monospace, `_…_` notes as italic, reduce markdown links to their visible text, treat `|`-rows as tables (row 2 is the `---` separator). See the script used for Uganda in the session history if needed.
