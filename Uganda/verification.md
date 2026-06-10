# Uganda BOOST — verification & sign-off

**Purpose.** Record the data-quality / overcounting issues found while onboarding Uganda, the
**corrected Excel SUMIFS** that resolve each one, and a sign-off line for the domain experts
(Massimo et al.). The corrected formulas are to be applied to the workbook's `Executed` sheet
(and Massimo's source + the BOOST CCI reconciliation log, per README step 9). The DLT pipeline
[UGA_transform_load_dlt.py](UGA_transform_load_dlt.py) implements the **identical** corrected,
mutually-exclusive criteria, so Excel and pipeline agree by construction.

> All overlap figures below were produced empirically by [UGA_detect_overcounting.py](UGA_detect_overcounting.py)
> against the full 448,144-row microdata (uncorrected workbook formulas). After the corrected,
> mutually-exclusive predicates were applied per line, a re-check over the same data found
> **0 lines matched by >1 econ category and 0 by >1 func category** — the partition is order-proof.
> The pipeline enforces this permanently via the `exactly_one_econ` / `exactly_one_func` expectations
> in [UGA_transform_load_dlt.py](UGA_transform_load_dlt.py).

---

## 1. Method & validation

- **Per-line tagging.** Each microdata line is assigned exactly **one** `econ`, `econ_sub`, `func`,
  `func_sub`. The Excel `Executed` sheet instead computes 273 `EXP_*` totals with independent `SUMIFS`,
  which lets a single line fall into several categories (the "no else-if" overcounting). Per-line
  tagging forces a partition.
- **Order-proof, no priority.** Each category is a **mutually-exclusive predicate, disjoint by
  construction** — every overlap is resolved by an explicit *discriminating criterion* baked into the
  predicate, not by clause order. Reordering the branches changes nothing.
- **Formula→Python validated.** The detector parses each `(code, year)` SUMIFS and reproduces the Excel
  cached cell: **1,209 / 1,216 (code, year) cells match within 0.5%** (99.4%). The 7 residual mismatches
  are non-blocking and explained in §6.
- **Formulas vary by year (critical).** 106 / 273 codes change criteria at the **FY2022/23 (column T)**
  vote/sector recode. All corrections below are stated for *both* coding eras where relevant.

---

## 2. Year-coding eras (context for every correction)

| Concept | ≤ FY2021/22 (cols C–S) | ≥ FY2022/23 (cols T–U) |
|---|---|---|
| Sector driver | `func0` named sectors ("04 Works and Transport", "08 health", "07 Education", …) | `func0` renumbered ("06 Natural Resources/…/Water", "10 …Housing", "12 Human Capital Development") + per-line **flags** |
| Health | `func0 "08 health"` | `health,"y"` flag |
| Education | `func0 "07 Education"` | `education,"y"` flag |
| Defense | `Vote_Function "1101 National Defence (UPDF)"` | `Vote_Function "1601 …"` |
| Judiciary | `Vote_Function {"1237*","1251 …",…}` | `admin2 {"101 judiciary*"}` |
| Public safety | `func0 "12 Justice, Law and Order"` + exclusions | `security,"y"` flag |
| Transport | `func0 "04 Works and Transport"` | `func0 "09 Integrated Transport Infrastructure And Services"` |
| Allowances | `econ5 "211103 Allowances"` | `econ5 "211106 Allowances (Incl. Casuals…)"` |

`func1` (COFOG) is **not** a usable driver: it is blank from ~FY2021/22 onward and, where populated,
does not reconcile with the Excel functional totals (the Excel reassigns judiciary/public-safety via
`Vote_Function` and social protection via the `SP`/`pension` flags). The corrections therefore mirror
the Excel's own drivers.

---

## 3. Corrected formulas — `econ` (and `econ_sub`)

Economic codes are **year-stable** (GFS `econ2` 21/22/23/24/25/26/31 did not change; only the allowances
`econ5` code did). The overcounting comes entirely from three cross-cutting columns — `assistance` (AI),
`pension` (Z), and the `add` override (W) — which the econ2-based SUMIFS fail to exclude.

**Classification decision (proposed):** lines flagged `assistance="y"` or `pension="y"` are **Social
benefits**, and lines carrying an `add` override belong to the override's category — so every other econ
predicate must exclude them. (Consistent with the "Social Assistance vs Wage/G&S/CapEx" precedent in the
Questions-for-Massimo doc.)

| Code | Category | Original SUMIFS (yr col C) | Corrected SUMIFS (add exclusions) | Overlap found (detector) |
|---|---|---|---|---|
| `EXP_ECON_WAG_BIL_EXE` | Wage bill | `SUMIFS(executed,year,C$1,econ2,"21*",pension,"<>y")+SUMIFS(executed,year,C$1,add,"wages")` | add `…,assistance,"<>y"` to **both** terms; add `…,add,"<>capital",add,"<>nonwage"` to the `econ2 21*` term | Wage ∩ Social benefits: **3 lines / 213.0M** (2005/06) |
| `EXP_ECON_USE_GOO_SER_EXE` | Goods & services | `SUMIFS(…,econ2,"22 USE OF GOODS AND SERVICES")+SUMIFS(…,add,"nonwage")` | add `…,pension,"<>y",assistance,"<>y"` to both; add `…,add,"<>wages",add,"<>capital"` to the `econ2 22` term | Goods ∩ Social benefits: **12 lines / 97.9M**; Goods ∩ Other grants: **70 lines / 15.89B** (2005/06) |
| `EXP_ECON_CAP_EXP_EXE` | Capital expenditures | `SUMIFS(…,econ2,"31*")+SUMIFS(…,econ2,"23 CONSUMPTION OF FIXED ASSETS")+SUMIFS(…,add,"capital")` | add `…,pension,"<>y",assistance,"<>y"` to all; add `…,add,"<>wages",add,"<>nonwage"` to the econ2 terms | CapEx ∩ Other grants: **49 lines / 24.87B** (2005/06) |
| `EXP_ECON_SUB_EXE` | Subsidies | `SUMIFS(…,econ2,"25 subsidies")` | add `…,pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage"` | (none in data, but criterion needed for disjointness) |
| `EXP_ECON_OTH_GRA_EXE` | Other grants/transfers | `SUMIFS(…,func1,"<>710 Social Protection",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264 …")` | add `…,pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage"` | ∩ Wage **134 lines / 3.79B**, ∩ Goods **70/15.89B**, ∩ CapEx **49/24.87B** (all 2005/06) — driven by `add` override on `econ2 26` lines |
| `EXP_ECON_INT_DEB_EXE` | Interest on debt | `SUMIFS(…,econ2,"24*")` | add `…,pension,"<>y",assistance,"<>y"` | (none in data; criterion for disjointness) |
| `EXP_ECON_OTH_EXP_EXE` | Other expenses | `C2 − SUM(C4,C8,C11,C15,C17,C21,C26)` (residual) | keep as residual = lines matched by none of the corrected predicates (now a clean complement) | — |

`econ_sub` overlaps are the same root cause and vanish with the same `assistance,"<>y"` exclusion:
Allowances ∩ Social Assistance **1 line / 2.8M**; Recurrent maintenance ∩ Social Assistance **1 line /
1.3M** (both 2005/06). Allowances criterion is the **year-union** `econ5 "211103*"` OR `"211106*"`.

- [ ] **SME sign-off (econ):** _______________________  date: __________

---

## 4. Corrected formulas — `func` (and `func_sub`)

### 4a. Health ∩ Education flag collision — **301 lines / 127.6B (FY2022/23–2023/24)** ⚠ largest functional overcount

From FY2022/23 the sector `func0 "12 Human Capital Development"` lumps health + education + social
development together (and `func1` COFOG is blank), so the Excel splits it with the independent flags
`health,"y"` and `education,"y"`. These flags are **not mutually exclusive** — 301 lines carry both.

| Code | Original (FY2022/23) | Corrected |
|---|---|---|
| `EXP_FUNC_HEA_EXE` | `SUMIFS(…,health,"y")` | `SUMIFS(…,health,"y",education,"<>y")` *(see decision below)* |
| `EXP_FUNC_EDU_EXE` | `SUMIFS(…,education,"y")` | `SUMIFS(…,education,"y",health,"<>y")` |

**Open question for Massimo (needs a rule):** for the 301 lines flagged *both* health and education
(`func0 "12 Human Capital Development"`), which COFOG wins? Options: (a) the better source signal is the
underlying `Vote`/`Program` text; (b) a fixed default (e.g. Health). The pipeline currently applies
**Health wins** as a placeholder so the partition is well-defined — replace once decided. The root fix is
to correct the `health`/`education` `IF(SEARCH(...))` flag formulas so they are disjoint at source (§5).

### 4b. Environmental protection ∩ Water & sanitation — **2,035 lines / 135.4B (multiple years)**

Water-supply lines are tagged both **Environment** (705 / new sector "06 Natural Resources… Land And
Water Management") and **Water & sanitation** (Housing 706). COFOG places water supply under **706
(Housing & community amenities)**.

| Code | Issue | Corrected |
|---|---|---|
| `EXP_FUNC_ENV_PRO_EXE` | env predicate captures water-supply `Vote_Function`s already owned by Water&sanitation | add water-supply `Vote_Function`/`wss` exclusions: `…,wss,"<>y"` and `…,Vote_Function,"<>0901* Water…",…` so water & sanitation stays in Housing |

Smaller func overlaps from the same flag/sector cross-cutting: Economic affairs ∩ Education **14 lines /
0.40B** (2022/23, the broad `func0 {01*..09*}` set vs the education flag → add `education,"<>y"`);
Environmental protection ∩ Social protection and Social protection ∩ Water&sanitation **1 line / 5.4M**
each (SP/pension flag — exclude social-protection-flagged lines from the sector predicates).

- [ ] **SME sign-off (func top):** _______________________  date: __________

### 4c. `func_sub` — intended hierarchy, resolved by specificity (not a bug)

These overlaps are **parent ⊃ child** by design; the per-line tag takes the **most specific** child:

| Parent ∩ child | lines | Σ executed | Resolution |
|---|--:|--:|---|
| Transport ⊃ Roads | 704 | 7.97T | tag **Roads** |
| Transport ⊃ Railroads | 2 | 292.4M | tag **Railroads** |
| Energy ⊃ Energy (power) | 954 | 2.82T | tag **Energy (power)** |
| Energy ⊃ Energy (oil & gas) | 639 | 1.31T | tag **Energy (oil & gas)** |

No `rail`/`air`/`water`-transport flag collisions were found in the data (the flags are effectively
disjoint in practice). The Excel parent codes legitimately keep the rollup; only the **per-line** tag is
made specific. No Excel change required here — documented for transparency.

- [ ] **SME sign-off (func_sub):** _______________________  date: __________

---

## 5. Helper-flag formula fixes (root cause of §4a)

The per-line `IF(SEARCH(...))` flag columns are themselves the root cause of the functional collisions.
Recommended corrected flag definitions (apply in the Expenditure sheet):

- `education` (AF) and `health` (AE): add a mutual exclusion so a "12 Human Capital Development" line
  cannot be both — e.g. compute `education` first, then `health = IF(AND(<health search>, education<>"y"), "y", "")`
  (or vice versa per the §4a decision).
- Audit over-broad `SEARCH` terms (the Questions-for-Massimo doc flags this pattern generically, e.g.
  a keyword matching unintended programs). Confirm each flag's search string against the FY2022/23
  vote/program text.

- [ ] **SME sign-off (helper flags):** _______________________  date: __________

---

## 6. Known validation caveats (non-blocking)

The 7 of 1,216 `(code, year)` cells the detector does not reproduce within 0.5%, with cause:

| Code(s) | Years | Cause (not an overcounting issue) |
|---|---|---|
| `EXP_FUNC_HOU_EXE` | 2005/06, 2022/23, 2023/24 | Excel value = SUMIFS **+ a composite cell ref** (`+C205` water&sanitation); the detector evaluates only the SUMIFS term. Composition is correct in the pipeline. |
| `EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE` | 2022/23, 2023/24 | Subnational cross-code with a doubled SUM structure in the workbook; out of scope for the flat-dimension partition. |
| `EXP_FUNC_ENV_PRO_EXE`, `EXP_FUNC_ENV_PRO_NEC_EXE` | 2022/23 | New-coding env predicate has long `Vote_Function` exclusion lists; the detector slightly over-includes — to reconcile once the §4b exclusions are finalised. |

None affects the econ/econ_sub/func/func_sub leaf predicates used for the partition.

---

## 7. Open questions for Massimo

1. **Health vs Education tie (§4a):** rule for the 301 `func0 "12 Human Capital Development"` lines
   flagged both. (Pipeline default: Health.)
2. **Water supply COFOG home (§4b):** confirm water & sanitation is **706 Housing** (not 705
   Environment) so the env predicate can exclude it.
3. **`add` override authority (§3):** confirm that an `add` value ("wages"/"capital"/"nonwage") overrides
   the line's `econ2`-based class (so `econ2 26 GRANTS` + `add wages` → Wage, not Other grants).
4. **Debt repayment:** `EXP_ECON_DEB_REP_EXE` (econ5 redemption codes) is excluded from the total; the
   pipeline drops these lines from `boost_gold`. Confirm.

---

## 8. Sign-off

| Reviewer | Role | Decision | Date |
|---|---|---|---|
|  |  | ☐ approved ☐ changes requested |  |
