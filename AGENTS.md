# Onboarding a new country — AI workflow guide

This file tells an AI agent (Claude Code, or any coding assistant) how to
onboard a new country into the mega-boost pipeline. Zimbabwe is the
reference implementation — every step below points at a concrete file there.

## Prerequisites

Before starting, the user should provide:

1. **The country's BOOST Excel workbook** (usually ~5–30 MB). They'll drop
   it under `temp/` locally; add `temp/` to `.gitignore` if not already.
2. **The 3-letter ISO country code** (e.g. `ZWE`, `ALB`, `KEN`).
3. **The country name** (e.g. `Zimbabwe`).

If any of these is missing, **ASK the user**.

## Core principles (do NOT violate)

1. **Report, don't interpret.** If an Excel formula looks wrong, flag it
   in `<Country>/_analysis/reports/ISSUES.md` — never silently patch.
2. **Translate formulas literally.** The pipeline mirrors Excel SUMIFS
   criteria verbatim; any deviation is a reportable issue.
3. **Driver CSVs, not code.** Country-specific classification logic lives
   in `tag_rules.csv` and `code_dictionary.csv`, not in the DLT notebook.
4. **Don't mutate the source workbook.** SMEs correct upstream; scripts
   only read and re-derive.
5. **Every country has different raw schemas.** Do not assume columns
   from another country exist. Inspect this country's data first.
6. **ASK the user when you hit a decision.** Don't guess named-range
   mappings, code conventions, or admin tiers.

## Reference implementation

Zimbabwe's `_analysis/` is the canonical example. When adapting to a new
country, **copy the Zimbabwe scripts as the template** and change:

- File paths (`Zimbabwe` → `<Country>`, `Zimbabwe BOOST.xlsx` → actual filename)
- `EXP_MAP` / `REV_MAP` — named-range to raw-column mapping
- `ECON_TOKENS` / `FUNC_TOKENS` — the country's code decomposition
- DLT table names (`zwe_*` → `<iso3>_*`)

Everything else should work unchanged. Only change logic when a script
fails or produces wrong results for this country.

## Workflow

### Phase 0 — Scaffold the country folder

```
<Country>/
├── <ISO3>_extract_microdata_excel_to_csv.py   # raw CSV dump (mirrors Albania/Kenya)
├── <ISO3>_transform_load_raw_dlt.py           # bronze/silver/gold (mirrors Zimbabwe)
└── _analysis/
    ├── README.md
    ├── reports/         # human-readable outputs only
    ├── data/            # machine CSVs consumed by the pipeline
    └── scripts/         # 1_*.py … 6_*.py
```

Copy `Zimbabwe/_analysis/scripts/*.py` and the `README.md` over.
Update the hardcoded `ROOT = Path(".../Zimbabwe/_analysis")` to this
country's folder. Also update the XLSX path in each script.

### Phase 1 — Workbook reconnaissance

Run `scripts/1_extract_workbook.py`.

Outputs:
- `data/sheet_inventory.csv` — every sheet with role guess
- `data/defined_names.csv` — named ranges (the data dictionary)
- `data/formula_map.csv` — every formula in the formula sheets
- `reports/workbook_inventory.md`

**Inspect `sheet_inventory.csv` manually.** The script's role-guess is a
heuristic. ASK the user:
- Which sheets contain **raw microdata** (Expenditure, Revenue, etc.)?
- Which sheets contain **formula output** (Approved, Executed, or named
  differently in this country)?
- Is there a separate data-dictionary sheet?

Other countries' conventions vary:
- Zimbabwe: `Expenditure`, `Revenue`, `Approved`, `Executed`.
- Albania: `Data_Expenditures`, `Data_Revenues`.
- Kenya, Colombia: each different.

Update `FORMULA_SHEETS` and the extract notebook's `RAW_SHEETS` accordingly.

### Phase 2 — Hardcoded/override audit

Run `scripts/2_audit_hardcoded.py`.

Looks for numeric literals inside formula columns — Excel's "SME typed a
number over the formula" pattern. These are the primary discrepancy source
in every country.

Output: `data/hardcoded_columns.csv`, `data/hardcoded_cells.csv`.

If `hardcoded_cells.csv` is non-empty, those are override cells that the
pipeline cannot reproduce. They'll be reported in `ISSUES.md` — do NOT
attempt to replicate them.

### Phase 3 — Tag-rule extraction (the critical phase)

Run `scripts/3_extract_tag_rules.py`.

Parses every SUMIFS in the formula sheets into structured criteria:

```json
{"field": "econ3", "op": "=", "value": "606"}
```

Output: `data/tag_rules.csv` — one row per formula sheet × row, with:
- `code`, `category` — BOOST classification tag identifier + human label
- `criteria_json` — the SUMIFS criteria
- `measure` — which named range is being summed (e.g. `approved`)
- `row_type` — `TAG` (SUMIFS), `ROLLUP` (plain SUM), `HEADER` (no formula)

**Inspect the fields used in criteria** (printed to console). ASK the user
about the **named-range → raw-column mapping**:

```python
# Zimbabwe's EXP_MAP (from defined_names.csv + Expenditure columns):
EXP_MAP = {
    "year": "year", "admin1": "admin1",
    "econ1": "econ1", ..., "econ4": "econ4",
    "prog1": "prog1", ..., "prog3": "prog3",
    "Exclude": "Exclude", "type": "type",
    "approved": "Approved", "executed": "Executed",
}
```

The **left side** is the named range as it appears in the formula; the
**right side** is the actual column header in the raw sheet. These can
differ — the names are sometimes lowercase in formulas and capitalized
in columns. Verify against `defined_names.csv` + raw sheet headers.

Other countries may not use named ranges at all — Albania formulas
reference `Data_Expenditures!M:M` directly. The extractor handles both
patterns, but the mapping step still needs SME confirmation.

### Phase 4 — Issues report

Run `scripts/4_generate_issues.py`.

Produces `reports/ISSUES.md` with:
- Hardcoded overrides (numeric)
- Broken named ranges (`#REF!`)
- External-workbook refs
- Inconsistent formulas within a column
- Approved vs Executed structural differences
- Volatile functions (`INDIRECT`, `OFFSET`, etc.)
- Open SME questions

Review with the user. Anything flagged here is **report-only** and will
remain a discrepancy in the pipeline output.

### Phase 5 — Code dictionary

Run `scripts/5_build_code_dictionary.py`.

Decomposes each tag code into `(econ, econ_sub, func, func_sub)` using
country-specific token dictionaries. The Zimbabwe version:

```python
ECON_TOKENS = {
    "WAG_BIL":    ("Wage bill", None),
    "BAS_WAG":    ("Wage bill", "Basic wages"),
    "CAP_EXP":    ("Capital expenditures", None),
    ...
}
```

**This is the most country-specific piece.** You'll need to:
1. Dump the distinct TAG codes from `tag_rules.csv`.
2. Inspect the code strings — they usually encode the taxonomy (e.g.
   `EXP_ECON_WAG_BIL_EXE` breaks into `EXP` / `ECON` / `WAG_BIL`).
3. Build the token dictionary by reading the workbook's `category` labels
   (which give the human-readable tag for each code).
4. **ASK the user to review** the token dictionary before relying on it.

CROSS codes (econ × func intersections) should be excluded from the
dictionary to prevent double-counting — they're covered by the
standalone ECON_* and FUNC_* codes.

Also appends §8 "Suspicious tag rules" to `reports/ISSUES.md`: duplicate
criteria, admin1/function mismatches.

### Phase 6 — Overlap audit

Run `scripts/6_detect_overlaps.py`.

Applies each tag rule to the raw sheet using pandas (local, no Spark).
For every peer pair at the same classification level:
- Computes the intersection of raw rows.
- If non-empty → reports the overlap with years and admin1 breakdown.

Output: `reports/overlap_report.md`, `data/overlap_detail.csv`.

Overlaps indicate double-count risk in the cross-country aggregator. The
fix is **always** in the dictionary (promote a peer to be a parent, or
drop a redundant code) or the workbook (consolidate duplicate-criteria
codes) — **never** in the DLT notebook.

### Phase 7 — DLT notebooks

Copy [Zimbabwe/ZWE_transform_load_raw_dlt.py](Zimbabwe/ZWE_transform_load_raw_dlt.py) and
[Zimbabwe/ZWE_extract_microdata_excel_to_csv.py](Zimbabwe/ZWE_extract_microdata_excel_to_csv.py).

Edit:
- Country name, ISO3, paths.
- `EXP_NAMED_RANGE_TO_COL` / `REV_NAMED_RANGE_TO_COL` (= `EXP_MAP` / `REV_MAP`).
- DLT table names.
- `admin0` / `admin1` derivation rules (often country-specific — ASK).

The silver layer iterates `tag_rules.csv` and applies each rule's
`criteria_json` to raw data. The gold layer joins with
`code_dictionary.csv` to populate `econ/econ_sub/func/func_sub`. This
pattern is portable; only the maps change.

### Phase 8 — Discrepancy review

At the end of the DLT notebook, call
`build_review()` from [quality/discrepancy_review.py](quality/discrepancy_review.py)
to auto-generate `quality/_reviews/<iso3>_discrepancy_review.md` on every
pipeline run. Preserves SME resolutions across iterations.

This utility is already country-agnostic — just parameterize the call
with the country code, dimension columns, and Excel sheet name.

### Phase 9 — Register in cross-country pipeline

Add the ISO3 code to [cross_country_aggregate_dlt.py:13](cross_country_aggregate_dlt.py#L13).

Confirm with user before doing this — it triggers inclusion in the
cross-country aggregate `boost_gold` table. Only do it after overlap
report is clean and discrepancy review is below 5%.

## Decision checkpoints (ASK the user)

| When | What to ask |
|---|---|
| Phase 0 | ISO3 code, country name, workbook filename |
| Phase 1 | Which sheets are raw data, which are formula output |
| Phase 3 | Confirm named-range → raw-column mapping |
| Phase 5 | Review generated `code_dictionary.csv`; confirm token mappings |
| Phase 6 | If overlaps detected, decide: promote to parent / drop redundant / leave |
| Phase 7 | How to derive `admin0`/`geo0` (Central vs Regional) |
| Phase 9 | Ready to merge to cross-country aggregate? |

## Things that vary wildly across countries (don't assume)

- **Sheet names** — no two countries share them.
- **Raw data schema** — some have one big sheet, others have many.
- **Formula style** — named ranges vs direct sheet refs.
- **Taxonomy encoding** — some encode in codes (Zimbabwe), some in labels
  (Colombia), some in text categories (South Africa).
- **admin0 derivation** — some have a Central/Regional flag, some imply
  it from admin1 values, some only have Central.
- **Year span & column layout** — wide vs. long format.
- **Missing-value markers** — `..`, `—`, blank, `N/A`.

See the "Country-Specific Pitfalls" section of each country's folder and
[Albania/ALB_transform_load_raw_dlt.py](Albania/ALB_transform_load_raw_dlt.py) /
[Colombia/transform_load_raw_dlt.py](Colombia/transform_load_raw_dlt.py) /
[Burkina_Faso/BFA_transform_load_dlt.py](Burkina_Faso/BFA_transform_load_dlt.py)
for examples of how different their logic is.

## Outputs this workflow should produce

Before the country is ready to merge:

- [ ] `<Country>/_analysis/reports/ISSUES.md` reviewed by SME
- [ ] `<Country>/_analysis/reports/overlap_report.md` reviewed by SME
- [ ] `<Country>/_analysis/data/tag_rules.csv` + `code_dictionary.csv` committed
- [ ] `<ISO3>_extract_microdata_excel_to_csv.py` runs on Databricks
- [ ] `<ISO3>_transform_load_raw_dlt.py` produces `<iso3>_boost_gold`
- [ ] `quality/_reviews/<iso3>_discrepancy_review.md` below 5% threshold
- [ ] Country code added to `cross_country_aggregate_dlt.py`

Only when all boxes are ticked should the PR be opened.

## Anti-patterns (things an AI agent might be tempted to do — DON'T)

1. **Generalizing the code dictionary prematurely.** Token abbreviations
   differ per country. Build each country's dictionary from scratch.
2. **Fixing Excel formulas in Python.** If you see a bug, flag it in
   ISSUES.md. The SME fixes the workbook upstream.
3. **Inferring classification from raw data.** The workbook's SUMIFS
   criteria are authoritative. Don't derive econ/func from raw column
   values except where the workbook itself does so.
4. **Skipping the overlap audit.** Even "small" countries have
   double-count patterns. Always run it.
5. **Running `scripts/5_build_code_dictionary.py` before
   `scripts/4_generate_issues.py`.** The numeric order matters — Phase 4
   writes the base ISSUES.md, Phase 5 appends §8. Running out of order
   wipes §8.
