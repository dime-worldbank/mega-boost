# Onboarding a new country — AI workflow guide

This file tells an AI agent (Claude Code, or any coding assistant) how to
onboard a new country into the mega-boost pipeline. Zimbabwe is the
reference implementation and Moldova is a second-generation example with
harder edge cases (multi-sheet raw data, bilingual filters, meta-rollups);
every step below points at concrete files in both.

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

**Resolve paths relative to `__file__`**, not absolute:

```python
ROOT = Path(__file__).resolve().parent.parent          # <Country>/_analysis
XLSX = ROOT.parent.parent / "temp" / "<Country> BOOST.xlsx"
```

Absolute paths like `Path("/Users/…/Zimbabwe/_analysis")` break as soon
as the repo is checked out somewhere else — another dev machine, CI,
Databricks Repos — and on Databricks you can only reliably reference
files in the same repo folder. `__file__`-relative resolution works
everywhere. Same pattern applies to the DLT notebook's driver-CSV
lookup (see `Moldova/MDA_transform_load_raw_dlt.py:ANALYSIS_DIR`).

Also update the XLSX filename (`Zimbabwe BOOST.xlsx` → actual filename)
and any hard-coded country-name strings in script comments / docstrings.

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
- **Moldova: raw data split across three year-range sheets** (`2006-15`,
  `2016-19`, `2020-24`) with different column schemas and bilingual filter
  values (English pre-2016, Romanian 2016+). A fourth hidden sheet `Raw2`
  uses yet another schema (`admin6` instead of `admin1`). Phase 1 surfaces
  all of this; confirm the split with the user.

Update `FORMULA_SHEETS` and the extract notebook's `RAW_SHEETS` accordingly.
Watch for **hidden sheets** — openpyxl reports `sheet_state == 'hidden'`;
these may still be referenced by formulas and must be handled or flagged.

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

Output: `data/tag_rules.csv` — one row per formula sheet × tag row × distinct
formula-shape group, with:
- `code`, `category` — BOOST classification tag identifier + human label
- `criteria_json` — list-of-lists of SUMIFS criteria. Each inner list is
  one SUMIFS block; a raw row matches the rule if **any** inner list's
  criteria are all satisfied (i.e. Excel's `=SUMIFS(…) + SUMIFS(…)` is
  treated as an OR across blocks).
- `measure` — which named range is summed by the first SUMIFS block (e.g.
  `approved`, or `approved_16` for a Moldova-style suffixed variant).
  Determines which raw sheet the rule dispatches to.
- `block_measures` — pipe-separated list of the measure per block,
  useful for catching mixed-source rules that reference the hidden
  `Raw2` sheet in a second block.
- `years_covered`, `year_min`, `year_max` — the year columns that share
  this formula shape
- `n_sumifs` — how many SUMIFS blocks the formula contains. Still useful
  as a flag for SME review even though all blocks are captured in
  `criteria_json`.
- `row_type` — `TAG` (SUMIFS), `ROLLUP` (plain SUM), `HEADER` (no formula)

**The same code can emit multiple rows** when its formula shape changes
between year columns (e.g. Moldova: base / `_16` / `_20` variants with
different field names and filter-value languages). The extractor groups
year columns by a shape hash and emits one row per distinct shape, so the
silver layer can dispatch each to the matching raw sheet.

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

**When raw data is split into multiple sheets (Moldova pattern):** define
one mapping per year-range sheet, keyed by the measure suffix. See
[Moldova/_analysis/scripts/6_detect_overlaps.py](Moldova/_analysis/scripts/6_detect_overlaps.py#L45)
for `SHEET_MAPS = {"base": …, "16": …, "20": …}` and the
`classify_measure()` dispatch. The silver layer in
[Moldova/MDA_transform_load_raw_dlt.py](Moldova/MDA_transform_load_raw_dlt.py)
does the same thing for Spark.

**Parser watch-outs:**
- Array constants inside SUMIFS — `econ2, {"A","B","C"}` — need brace-aware
  splitting. The current parser respects `()` and `{}` depth when splitting
  on top-level commas; don't revert that.
- Filter values may be localized (e.g. Moldova's Romanian strings from 2016
  onward). The pipeline matches these literally — do not translate.

### Phase 4 — Issues report

Run `scripts/4_generate_issues.py`.

Produces `reports/ISSUES.md` with:
1. Hardcoded overrides (numeric) — per-cell with `code` + `category`
2. Broken named ranges (`#REF!`)
3. External-workbook refs
4. Approved vs Executed structural differences (full formulas, not truncated)
5. Volatile functions (`INDIRECT`, `OFFSET`, etc.)
6. Suspicious tag rules (appended by Phase 5)

Review with the user. Anything flagged here is **report-only** and will
remain a discrepancy in the pipeline output.

**Measure-agnostic normalizer** needs to cover country-specific measure
variants — Zimbabwe has `approved`/`executed`/`approved_orig`, Moldova
adds `approved_16`/`approved_20`/`executed_16`/`executed_20`. The regex in
`4_generate_issues.py` matches `(approved|executed|revised)(_\w+)?` so
suffixes like `_16` don't register as real shape differences. If a new
country uses a different measure naming convention, extend the regex.

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

**Meta-rollup codes** (codes that aggregate a whole COFOG or econ
category — e.g. Moldova's `EXP_ECON_TOT_EXP_EXE` = all expenditure,
`EXP_ECON_SBN_TOT_SPE_EXE` = all subnational spending,
`EXP_FUNC_ECO_REL_EXE` = all COFOG 704, `REV_ECON_TOT_EXE` = total
revenue) must also be excluded. Add them to the `META_ROLLUP_CODES` set
in `5_build_code_dictionary.py`. Excluding them matters for two
reasons:

1. In the overlap audit they would intersect every sub-code (noise).
2. In the per-row cascade silver (Phase 7 Moldova pattern) they sit
   near the top of the sheet with the broadest filter, so if not
   excluded the cascade assigns every row to the rollup code and
   classification granularity is lost.

**Alongside `code_dictionary.csv`, Phase 5 emits
`reports/code_hierarchy.md`** — a tree view grouping every code under
its assigned `econ`/`econ_sub` or `func`/`func_sub` with any
`decompose_notes` flags inline. This is the SME review artifact; show it
to the user before relying on the dictionary.

Also appends §6 "Suspicious tag rules" to `reports/ISSUES.md`: duplicate
criteria, admin1/function mismatches. (The marker also matches the older
`## 8.` heading for backward compatibility with reports generated before
the section renumbering.)

**Token-dictionary portability:** align labels with the canonical
vocabulary that other countries already feed into
[cross_country_aggregate_dlt.py:40 boost_gold](cross_country_aggregate_dlt.py#L40).
Moldova's Phase 5 docstring calls this out: the full list of reused
labels came from an Explore-agent survey of all 18 existing DLT
notebooks. Don't invent new labels unless the concept truly doesn't
exist elsewhere.

**Subnational pattern:** Moldova has `EXP_ECON_SBN_*` and `EXP_FUNC_SBN_*`
codes that filter by `admin1 ∈ {"local","locale"}` — they are
cross-cutting admin×category tags, not pure econ or func codes. The
dictionary flags them with a `decompose_notes` entry ("Moldova
subnational tag — confirm admin0/geo0 handling"); Phase 6 reports their
overlaps in a separate SBN section.

### Phase 6 — Formula overcounting audit

Run `scripts/6_detect_overlaps.py`.

Applies each tag rule to the raw sheet using pandas (local, no Spark).
For every pair of codes qualifying at the same classification depth
(`econ` rollup, `econ_sub`, `func` rollup, `func_sub`) it computes the
intersection of raw rows and reports any non-empty overlap with:
- `Σ overcounted` — sum of each raw measure over intersection rows. This
  is the exact amount that gets double-counted if both codes are summed
  together — not A−B. Accompanied by each code's full total and the
  ratio `overcounted ÷ min(A,B)` so severity is scannable.
- `rows in overlap` — cardinality of the double-count set.

**Pair semantics** (important, iterated on across Zimbabwe → Moldova):
- Same-depth only. A func-level rollup is never paired with one of its
  func_sub children (that's a parent/child, not an overcount).
- Parent-agnostic at sub-levels. `econ_sub=Basic wages` is compared with
  `econ_sub=Recurrent maintenance` even though they have different econ
  parents, because both are claims about mutually-exclusive sub-econs.
- **Same-value pairs** (two codes share the level value) expose
  duplicates: if `Σ overcounted == Σ code A == Σ code B`, the filters are
  effectively identical.
- **Cross-category pairs** (same level, different values) expose real
  overcounting bugs: the categories are meant to be mutually exclusive,
  so any intersection is a workbook-level classification mistake.
- Subnational `SBN_*` codes are compared only against other SBN codes,
  in a dedicated section — never mixed with the primary pairs.

Output: `reports/overlap_report.md` ("Moldova formula overcounting
report"-style title, not "raw-data overlaps" — the name must reflect
what it measures), `data/overlap_detail.csv`.

Overlaps indicate double-count risk in the cross-country aggregator. The
fix is **always** in the dictionary (move a rollup code to
`META_ROLLUP_CODES`, fix a token mapping, or drop a redundant code) or
the workbook (consolidate duplicate-criteria codes) — **never** in the
DLT notebook.

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

**Multi-sheet dispatch (Moldova pattern):** when raw data is split across
sheets, emit one bronze per sheet and dispatch each rule to its bronze
based on the rule's `measure` suffix. See
[Moldova/MDA_transform_load_raw_dlt.py](Moldova/MDA_transform_load_raw_dlt.py):
three `MAP_BASE` / `MAP_16` / `MAP_20` dictionaries and a
`classify_measure()` helper. EXP vs REV are both pulled from the same
bronzes (Moldova distinguishes them via `econ0_* ∈ {"Expenditures",
"Revenues"}`).

**Silver layer — per-row tagging, NOT SUMIFS aggregation.** Zimbabwe's
original template iterates each tag rule and aggregates via SUMIFS,
producing `(year, code, approved, executed)` rows. This is fine when
codes don't overlap, but countries like Moldova have codes whose filters
share raw rows — summing across codes then double-counts. The right
shape is Albania's: emit one silver row per raw expenditure record and
attach classification labels by **two parallel if-else cascades**:

- `econ_code`: first matching `EXP_ECON_*` rule (REV rules for revenue
  silver), scanned in workbook sheet-row order
- `func_code`: first matching `EXP_FUNC_*` rule, same scan

Each raw row thus carries at most one econ and one func label; summing
across either dimension reproduces the corresponding Excel total
exactly, and the dimensions are independent. A uniform pre-filter
(`transfer` keep-rule + `econ0 ∈ {"Expenditures","Revenues"}` for 2016+)
drops intra-budgetary transfers before tagging.

**Multi-SUMIFS rules are OR'd.** When an Excel cell is
`=SUMIFS(…) + SUMIFS(…)`, the two blocks are alternative ways for a raw
row to be included — so Phase 3's `criteria_json` is a
*list-of-lists* (each inner list = one SUMIFS block's criteria) and the
silver's `_rule_match_expr` OR's across branches. Don't collapse this
back to a single-criterion rule; you'll miss rows the Excel formula
captures via the second SUMIFS.

**Priority (= code order) is intentional.** When two rules share rows,
the earlier-in-sheet rule wins the tag. `reports/overlap_report.md` is
the audit artifact that surfaces these tie-breaks so the SME can
confirm or reorder — do **not** re-sort rules by specificity or try to
resolve overlaps in code.

**Unmatched rows stay in silver with code=NULL.** A row that passes the
uniform filter but no cascade rule matches surfaces as a coverage hole
(NULL econ_code or func_code). Left-join to `code_dictionary.csv` in
gold so NULL codes become NULL labels — visible in the output.

**Do not include a `build_review()` call in the DLT notebook.** The
discrepancy review runs in local testing, not as part of the Spark
pipeline — keep the DLT file strictly bronze/silver/gold.

**Pin types explicitly on the final gold select.**
[cross_country_aggregate_dlt.py:40](cross_country_aggregate_dlt.py#L40)
asserts that every country's `{iso}_boost_gold` has `approved` and
`executed` as `DoubleType` and hard-fails the whole cross-country union
otherwise. Even when bronze already uses `DoubleType`, intermediate
ops (joins, NULL coercion under `unionByName(allowMissingColumns=True)`,
etc.) can shift types — so cast every output column on the final
`.select(...)`:

```python
.select(
    col("country_name").cast("string"),
    col("year").cast("int"),
    col("admin0").cast("string"), col("admin1").cast("string"),
    col("admin2").cast("string"), col("geo0").cast("string"),
    col("geo1").cast("string"),
    col("func").cast("string"), col("func_sub").cast("string"),
    col("econ").cast("string"), col("econ_sub").cast("string"),
    col("approved").cast("double"),
    col("revised").cast("double"),
    col("executed").cast("double"),
)
```

Same pattern Kenya uses at [Kenya/KEN_transform_load_dlt.py:236](Kenya/KEN_transform_load_dlt.py#L236)
and Moldova at [Moldova/MDA_transform_load_raw_dlt.py](Moldova/MDA_transform_load_raw_dlt.py).

**Also pin the bronze schemas.** Pass a `StructType` via
`spark.read.format("csv").schema(...)` and drop `inferSchema=true`.
`inferSchema` re-scans the full CSV on the driver just to guess types
(~1 GB of driver memory on a 186 MB file — a real OOM risk on modest
clusters), and any misguess propagates through silver into gold where
the cross-country type assertion catches it anyway.

### Phase 8 — Discrepancy review (local)

Run the discrepancy review as a local test (not inside the DLT
notebook). `build_review()` from [quality/discrepancy_review.py](quality/discrepancy_review.py)
compares pipeline silver output against the Excel Executed sheet at
`(year, code)` granularity, preserves SME resolutions across runs, and
writes `quality/_reviews/<iso3>_discrepancy_review.md`. Parameterize
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

- **Sheet names** — no two countries share them. Hidden sheets exist.
- **Raw data schema** — some have one big sheet, others have many.
  Moldova splits raw data by year range with different columns in each.
- **Formula style** — named ranges vs direct sheet refs; array constants
  `{"A","B",…}` inside SUMIFS; measure names with year-range suffixes.
- **Filter-value language** — may switch mid-workbook (Moldova: English
  pre-2016, Romanian 2016+). Translate nothing; match literally.
- **Taxonomy encoding** — some encode in codes (Zimbabwe, Moldova), some
  in labels (Colombia), some in text categories (South Africa).
- **Meta-rollup codes** — some workbooks include "total-of-a-category"
  codes (`ECO_REL` = all COFOG 704 in Moldova, `TOT` = total revenue)
  that must be excluded from the dictionary like CROSS codes.
- **Code-name drift from category** — Moldova has
  `EXP_FUNC_REV_CUS_EXC_EXE` whose category label is actually
  "Recreation, culture and religion (COFOG 708)". When code name and
  category disagree, the category wins; flag in ISSUES.md.
- **admin0 derivation** — some have a Central/Regional flag, some imply
  it from admin1 values, some only have Central.
- **Year span & column layout** — wide vs. long format.
- **Missing-value markers** — `..`, `—`, blank, `N/A`.
- **Hidden supplemental sheets** — Moldova's workbook has a hidden
  `Raw2` sheet (admin6 schema, no econ0) referenced by ~40 formulas as a
  `+ SUM(SUMIFS('Raw2'!…))` supplement. These can be surfaced by
  `openpyxl`'s `sheet_state == 'hidden'`. If the supplement is small the
  pragmatic choice is to skip it and document the coverage gap in
  `parsing_verification.md` and the DLT header (Moldova's approach). If
  it matters for downstream numbers, add a fourth bronze + mapping.
- **Cell-arithmetic around SUMIFS** — `=SUMIFS(…) - C19` subtracts a
  sibling cell's value to disambiguate sub-categories. The parser only
  captures the SUMIFS; the `-cell_ref` is silently dropped. Surfaces as
  a MISMATCH in `reports/parsing_verification.md`. Resolution is either
  SME-side (split the combined formula into two codes) or a parser
  extension that evaluates referenced cells.

See the "Country-Specific Pitfalls" section of each country's folder and
[Albania/ALB_transform_load_raw_dlt.py](Albania/ALB_transform_load_raw_dlt.py) /
[Colombia/transform_load_raw_dlt.py](Colombia/transform_load_raw_dlt.py) /
[Burkina_Faso/BFA_transform_load_dlt.py](Burkina_Faso/BFA_transform_load_dlt.py) /
[Moldova/MDA_transform_load_raw_dlt.py](Moldova/MDA_transform_load_raw_dlt.py)
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
   differ per country. Build each country's dictionary from scratch but
   reuse the canonical `econ`/`func` labels from the existing DLT
   notebooks (survey them with Explore before picking labels).
2. **Fixing Excel formulas in Python.** If you see a bug, flag it in
   ISSUES.md. The SME fixes the workbook upstream.
3. **Inferring classification from raw data.** The workbook's SUMIFS
   criteria are authoritative. Don't derive econ/func from raw column
   values except where the workbook itself does so.
4. **Skipping the overlap audit.** Even "small" countries have
   double-count patterns. Always run it.
5. **Running `scripts/5_build_code_dictionary.py` before
   `scripts/4_generate_issues.py`.** The numeric order matters — Phase 4
   writes the base ISSUES.md, Phase 5 appends the "Suspicious tag rules"
   section. Running out of order wipes it.
6. **Pairing parent with child in the overlap audit.** A func-rollup code
   (`func_sub = None`) and a func_sub code under the same func share raw
   rows by definition — that's hierarchy, not overcounting. Same-depth
   pairing only (econ↔econ rollups, econ_sub↔econ_sub siblings, …).
7. **Reporting A−B as the "gap" in overlap audit.** The relevant number
   is the double-count: `Σ measure over rows matched by BOTH codes`.
   A−B is noise.
8. **Translating localized filter values.** Moldova's Romanian filter
   strings (`"Cu exceptia transferurilor"`) must be preserved verbatim
   — the pipeline matches them literally against the raw column.
9. **Mixing subnational codes with the primary overlap report.** SBN
   codes (admin × category) go in their own section and are compared
   only with other SBN codes.
10. **Putting `build_review()` in the DLT notebook.** The discrepancy
    review is a local-test step, not a pipeline stage.
11. **Using SUMIFS aggregation for the silver layer when codes overlap.**
    Zimbabwe's template works because its codes are near-mutually-
    exclusive. If the overlap audit surfaces real intersections (Moldova
    did), switch to the per-row if-else cascade pattern — each raw row
    gets at most one `econ_code` and one `func_code`, and summing
    downstream can't double-count.
12. **Collapsing multi-SUMIFS rules to their first block.** Excel cells
    like `=SUMIFS(…) + SUMIFS(…)` are a single rule with an OR across
    blocks. Only extracting the first SUMIFS silently undercounts
    whatever the second block covers. `criteria_json` must be a
    list-of-lists.
13. **Shipping the gold table without explicit type casts.** The
    cross-country aggregator asserts `approved` and `executed` are
    `DoubleType`. Bronze alone isn't enough — the final `gold.select(...)`
    must cast every column, or a silent type drift through
    joins/unions will fail the aggregator's assertion and block the
    country from merging.
