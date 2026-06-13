# Uganda BOOST — verification & sign-off

**Purpose.** Record each overcounting / data-quality issue found while onboarding Uganda as a
self-contained block: the **quantified overlap**, the **original Excel formula**, and the
**proposed corrected formula**, side by side. The corrected formulas are to be applied to the
workbook's `Executed` sheet (and Massimo's source + the BOOST CCI reconciliation log, per README
step 9). The pipeline [UGA_transform_load_dlt.py](UGA_transform_load_dlt.py) already implements the
identical corrected, mutually-exclusive criteria.

### How to read this document

Each issue is one block:

> **`<ID>` · short title** — code, dimension, era, the measured overlap, and the cause, followed by
> an **Original (Excel)** code block and a **Proposed (corrected)** code block, the exact criteria
> added, and a sign-off checkbox.

**Where each formula lives.** Every `EXP_*` code is one **row** of the `Executed` sheet: the code is in
column **A**, its label in column **B**, and the formula is repeated across the **year columns C→U**
(FY2005/06 → FY2023/24). The formula shown is the column-**C** (FY2005/06) cell; in each later column the
year reference shifts (`C$1`→`D$1`→…→`U$1`) but the criteria are identical *within an era*. The
**≥FY2022/23** variant lives in columns **T:U**. So "Cell `Executed!C4:U4`" means: edit the formula in
cells C4 through U4 of row 4 (apply the ≤FY2021/22 fix to C4:S4 and the ≥FY2022/23 fix to T4:U4 where the
eras differ). Helper-flag fixes (§7) are on the **`Expenditure`** sheet columns, applied to every data row.

---

## 1. Method & validation

- **Per-line tagging, order-proof.** Every microdata line gets exactly one `econ`/`econ_sub`/`func`/
  `func_sub`. Each category is a **mutually-exclusive predicate, disjoint by construction** — no
  reliance on clause order. Verified on all 448,144 rows: **0 lines match >1 econ category, 0 match
  >1 func category**. The pipeline enforces this via the `exactly_one_econ`/`exactly_one_func`
  expectations.
- **Formula→Python validated:** the detector ([UGA_detect_overcounting.py](UGA_detect_overcounting.py))
  parses each `(code, year)` SUMIFS and reproduces the Excel cached cell **literally** (Excel text
  semantics: case-insensitive, trailing spaces significant) for **3,088 / 3,090 (99.9%)** across all
  19 years. The remaining **2** are a **confirmed Excel formula error** ([X1](#x1)) where the workbook
  double-counts and the detector's value is the correct one — so the parser is correct on **3,090 / 3,090**.
- **Shared formulas (important):** the Executed sheet stores each row's formula once, on the col-C
  master cell (`<f t="shared" ref="C..:U..">`), with D:U inheriting it. The detector resolves these, so
  **every category is now validated and overlap-checked across all 19 years** — earlier figures that
  only covered ~3 years (2005/06 + the FY2022/23–23/24 era) badly understated the overlaps; the numbers
  below are the full-period values.
- **All overlap figures below are empirical**, measured by the detector against the full microdata.

---

## 2. Year-coding eras (apply each fix to the matching era)

| Concept | ≤ FY2021/22 (cols C–S) | ≥ FY2022/23 (cols T–U) |
|---|---|---|
| Sector driver | `func0` named sectors ("08 health", "07 Education", "04 Works and Transport") | `func0` renumbered ("06 …Water", "10 …Housing", "12 Human Capital Development") + per-line **flags** |
| Health / Education | `func0 "08 health"` / `func0 "07 Education"` | `health,"y"` / `education,"y"` flags |
| Defense | `Vote_Function "1101 …"` | `Vote_Function "1601 …"` |
| Judiciary / Public safety | `Vote_Function {"1237*",…}` / `func0 "12 Justice…"` | `admin2 {"101 judiciary*"}` / `security,"y"` |
| Transport | `func0 "04 Works and Transport"` | `func0 "09 Integrated Transport…"` |
| Allowances | `econ5 "211103 Allowances"` | `econ5 "211106 Allowances (Incl. Casuals…)"` |

`econ` is **year-stable** (GFS `econ2` unchanged); only the allowances `econ5` code differs, handled by a
union. `func` is **year-aware**. `func1` (COFOG) is unusable (blank from ~FY2021/22 and does not
reconcile with the Excel functional totals).

---

## 3. Overlap inventory (at a glance)

"Over-counted category" = the one whose total is **too high** and must come **down**; the partner
category **owns** the line and is **unchanged**.

| ID | Dim | Cell(s) | Over-counted category & cause | Reduction | Partner (unchanged) |
|---|---|---|---|---|---|
| [E1](#e1) | econ | `Executed!C4:U4` | **Wage bill** counts Social-benefit lines (`assistance`/`pension`) | −7.2B (60 lines, 19 yrs) | Social benefits |
| [E2](#e2) | econ | `Executed!C11:U11` | **Goods & services** counts Social-benefit lines | −12.3B (241 lines, 19 yrs) | Social benefits |
| [E3](#e3) | econ | `Executed!C8:U8` | **Capital expenditures** is NOT clean — it double-counts `add`-override lines in FY22/23–23/24 → see [E7](#e7) | (in [E7](#e7)) | Wage / Goods |
| [E4](#e4) | econ | `Executed!C15:U15` | Subsidies — no discrepancy; defensive disjointness criteria | 0 | — |
| [E5](#e5) | econ | `Executed!C21:U21` | **Other grants/transfers** counts `add`-override lines (Wage/Goods/CapEx) **and** Social-benefit lines | **−≈1.94T** (CapEx 970B + Wage 263B + Goods 179B + SocBen 524B) | Wage / Goods / CapEx / Social benefits |
| [E6](#e6) | econ | `Executed!C26:U26` | Interest on debt — no discrepancy; defensive disjointness criteria | 0 | — |
| [E7](#e7) | econ | `Executed!T4:U4`, `T8:U8`, `T11:U11` | **`add`-override collisions (FY22/23–23/24)** — a category's base `econ2` term counts a line whose `add` flag assigns it elsewhere | **Wage∩CapEx 6.95T; CapEx∩Goods 2.56T** | the `add`-named category |
| [F1](#f1) | func | `Executed!T215:U215`, `T235:U235` | **Health ∩ Education flag collision** | **301 lines / 127.6B (FY22/23–23/24)** | Health (Q1) |
| [F2](#f2) | func | `Executed!C193:U193`, `C199:U199` | **Environmental protection (705)** double-counts water-supply lines (COFOG **706**) | **−46.6B / 1,649 lines (13 yrs)** | Housing & community amenities |
| [F3](#f3) | func | `Executed!T41:U41` | Economic affairs ∩ Education (new broad sector set) | 14 lines / 0.40B (FY22/23) | Education |
| [F4](#f4) | func | `Executed!257` (SP/pension flags) | **Sectors** double-count pension lines that are Social protection (710) → each sector excludes `SP`/`pension`; SP keeps them | Educ −200.6B · PubOrd −192.7B · Health −183.2B · Eco −176.0B · Hou −19.5B · Env −9.6B | Social protection (710) |
| [FS1](#fs1) | func_sub | `Executed!63,76,91,130,143,155` | Transport⊃Roads / Energy⊃Power/Oil (hierarchy) — **DEFERRED** | resolved to most-specific |
| [H1](#h1) | flags | `Expenditure!AE,AF` | `health`/`education` flag definitions not disjoint | root cause of F1 |
| [X1](#x1) | cross | `Executed!T278:U278` | **Subnational CapEx in water&san** — Excel **array-broadcast double-count** (not an overlap) | −93.1B FY22/23, −104.4B FY23/24 | — |

---

## 4. `econ` issues

All econ overcounting comes from **two cross-cutting columns**, each fixed by one rule applied to *every*
econ formula (and to *every* SUMIFS term inside it):

**Rule 1 — Social benefits own the flagged lines.** A line with `assistance="y"` or `pension="y"` is
Social benefits. → every other econ formula (every term) adds `assistance,"<>y"` and `pension,"<>y"`.

**Rule 2 — the `add` override is authoritative.** The `add` column reclassifies a line as `wages`→Wage,
`capital`→CapEx, `nonwage`→Goods regardless of its `econ2`. So the line belongs to **exactly** that one
category. Each category's `add` term *claims* its own override value; every *other* category (and the
plain-`econ2` terms) *exclude* it. This is what resolves the **Other grants ∩ Wage / Goods / CapEx** web:

| `add` value | belongs to (its `add` term keeps it) | excluded from |
|---|---|---|
| `wages` | Wage bill ([E1](#e1)) | Goods [E2], CapEx [E3], Subsidies [E4], Other grants [E5], Interest [E6] |
| `capital` | Capital expenditures ([E3](#e3)) | Wage [E1], Goods [E2], Subsidies [E4], Other grants [E5], Interest [E6] |
| `nonwage` | Goods & services ([E2](#e2)) | Wage [E1], CapEx [E3], Subsidies [E4], Other grants [E5], Interest [E6] |

**Which total actually changes (important).** An `add`-override line **belongs to** the override's
category (Wage/Goods/CapEx). The catch is that **every category's base `econ2` term has no `add` guard**,
so it also grabs override lines that belong elsewhere:
- **≤FY2021/22:** the `add` column is sparse; the dominant over-count is **Other grants/transfers**
  (≈1.94T full-period, including the pension/assistance lines) — [E5](#e5).
- **≥FY2022/23:** the `add` column is dense and the base `econ2` terms of **Wage / Capital expenditures /
  Goods collide with each other** — **Wage ∩ CapEx 6.95T, CapEx ∩ Goods 2.56T** ([E7](#e7)). So
  Capital expenditures and Goods **are over-counted in the new era** — the earlier "CapEx reconciles, no
  discrepancy" verdict held only for the ~3 years the detector then covered.

Separately, the Social-benefit overlap lowers **Wage (−7.2B)** and **Goods (−12.3B)** across all years.
Subsidies, Interest and Social benefits have **no discrepancy** — their edits below are *defensive*.

So each issue below is just these two rules applied to one code. A category that has an `add` term
(Wage/Goods/CapEx) corrects **both** of its terms: the plain-`econ2` term becomes "pure `econ2`, no
override, not a Social benefit"; the `add` term becomes "the override lines, not a Social benefit".

<a id="e1"></a>
### E1 · Wage bill ∩ Social benefits — and Wage's `add="wages"` term ∩ Other grants / Goods / CapEx
- **Code** `EXP_ECON_WAG_BIL_EXE` · **Cell** `Executed!C4:U4` (label `B4`) · **Dim** econ · **Era** all years (econ2-driven)
- **Overlaps found (all 19 years):**
  - Wage ∩ Social benefits — **60 lines / 7.2B** (`assistance`/`pension` lines whose `econ2` is `"21*"`) → **this is what lowers Wage's total** (−7.2B).
  - Wage ∩ Other grants/transfers — **1,699 lines / 263B** (`econ2 "26 GRANTS"` lines tagged `add="wages"`): these **belong to Wage** (the `add` override), so **Wage keeps them, unchanged** — the double-count is removed from **Other grants** ([E5](#e5)), not Wage.
  - Wage ∩ Capital expenditures — **part of the 6.95T** `add`-override collision in FY2022/23–23/24 (`add="capital"` lines whose `econ2` is `"21*"`, and `add="wages"` lines whose `econ2` is `"31*"`) → see **[E7](#e7)**.
- **Cause:** Wage is **two** SUMIFS terms. **Term 1** (`econ2 "21*"`) excludes `pension` but not
  `assistance` (→ the 7.2B leak across all years). **Term 2** (`add,"wages"`) has **no filter at all**, so
  it grabs every `add="wages"` line — those are correctly Wage's, but Other grants must stop also counting
  them; in the new era Term 1 also grabs `add="capital"`/`"nonwage"` lines whose `econ2` is `"21*"` ([E7](#e7)).

**The formulas that count the same line:**
```excel
' >>> FIX THIS ONE  —  Wage bill (Executed!C4)
=SUMIFS(executed,year,C$1,econ2,"21*",pension,"<>y")   ' Term 1
 +SUMIFS(executed,year,C$1,add,"wages")                 ' Term 2 — no filter -> grabs add=wages everywhere

' ...also counted by  —  Social benefits (assistance/pension, no econ2 filter)
=SUMIFS(executed,year,C$1,assistance,"y")            ' Social Assistance (Executed!C18)
=SUM(SUMIFS(executed,year,C$1,pension,"y"))          ' Pensions          (Executed!C19)

' ...also counted by  —  Other grants/transfers (Executed!C21) for an econ2 26 line tagged add="wages"
=SUMIFS(executed,year,C$1,func1,"<>710 Social Protection",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264 To Resident Non-government units")
```
- `assistance="y"` + `econ2 "21*"` → counted by **both Wage (Term 1) and Social benefits**.
- `add="wages"` + `econ2 "26 GRANTS"` → counted by **both Wage (Term 2) and Other grants**.

**Proposed fix — apply Rule 1 to both terms, Rule 2 to Term 1; Wage keeps its `add="wages"` lines:**
```excel
=SUMIFS(executed,year,C$1,econ2,"21*",pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage")
 +SUMIFS(executed,year,C$1,add,"wages",pension,"<>y",assistance,"<>y")
```
- **Term 1 adds:** `assistance,"<>y"` (Rule 1) · `add,"<>wages",add,"<>capital",add,"<>nonwage"` (Rule 2 → pure `econ2 "21*"`, no override).
- **Term 2 adds:** `pension,"<>y",assistance,"<>y"` (Rule 1). Wage **keeps** the `add="wages"` lines; the
  Wage ∩ Other-grants/Goods/CapEx overlaps are removed by the **counterparts** excluding `add="wages"`
  ([E2](#e2)/[E3](#e3)/[E5](#e5)).
- [ ] **Sign-off:** ____________________

<a id="e2"></a>
### E2 · Goods & services leaks Social-benefit / Other-grants lines
- **Code** `EXP_ECON_USE_GOO_SER_EXE` · **Cell** `Executed!C11:U11` (label `B11`) · **Dim** econ · **Era** all years
- **Overlaps found (all 19 years):**
  - Goods ∩ Social benefits — **241 lines / 12.3B** → **what lowers Goods' total** (−12.3B).
  - Goods ∩ Other grants — **1,297 lines / 179B** (`econ2 "26"` lines tagged `add="nonwage"`): these **belong to Goods** (the `add` override), so **Goods keeps them, unchanged** — removed from **Other grants** ([E5](#e5)).
  - Goods ∩ Capital expenditures — **part of the 2.56T** `add`-override collision in FY2022/23–23/24 → see **[E7](#e7)**.
- **Cause:** the `econ2 "22…"` term doesn't exclude `assistance`/`pension` (→ the 12.3B leak across all
  years); the `add="nonwage"` term is correctly Goods', but Other grants must stop also counting those
  lines; in the new era the `econ2 "22…"` term also grabs `add="capital"` lines ([E7](#e7)).

**The formulas that count the same line:**
```excel
' >>> FIX THIS ONE  —  Goods & services (Executed!C11)
=SUMIFS(executed,year,C$1,econ2,"22 USE OF GOODS AND SERVICES")
 +SUMIFS(executed,year,C$1,add,"nonwage")

' ...also counted by  —  Social benefits (no econ2 filter)
=SUMIFS(executed,year,C$1,assistance,"y")            ' Social Assistance (Executed!C18)
=SUM(SUMIFS(executed,year,C$1,pension,"y"))          ' Pensions          (Executed!C19)

' ...also counted by  —  Other grants/transfers (Executed!C21) for econ2 26 lines tagged add="nonwage"
=SUMIFS(executed,year,C$1,func1,"<>710 Social Protection",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264 To Resident Non-government units")
```
A `econ2 "22…"` line with `assistance="y"` is counted by **both Goods & services and Social benefits**;
a `econ2 "26 GRANTS"` line with `add="nonwage"` is counted by **both Goods & services** (its
`add,"nonwage"` term) **and Other grants/transfers**.

**Proposed fix — Rule 1 on both terms, Rule 2 on the `econ2` term; Goods keeps its `add="nonwage"` lines:**
```excel
=SUMIFS(executed,year,C$1,econ2,"22 USE OF GOODS AND SERVICES",pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage")
 +SUMIFS(executed,year,C$1,add,"nonwage",pension,"<>y",assistance,"<>y")
```
- **`econ2 "22"` term adds:** `pension,"<>y",assistance,"<>y"` (Rule 1) · `add,"<>wages",add,"<>capital",add,"<>nonwage"` (Rule 2 → pure `econ2 "22"`, no override).
- **`add,"nonwage"` term adds:** `pension,"<>y",assistance,"<>y"` (Rule 1). The `econ2 "26"` line tagged `add="nonwage"` now stays only here; Other grants releases it (see [E5](#e5)).
- [ ] **Sign-off:** ____________________

<a id="e3"></a>
### E3 · Capital expenditures — clean in early years, but double-counts `add`-override lines from FY2022/23
- **Code** `EXP_ECON_CAP_EXP_EXE` · **Cell** `Executed!C8:U8` (label `B8`) · **Dim** econ · **Era** all years
- **Early era (≤FY2021/22): no discrepancy.** It reconciles with Excel exactly, e.g.
  **FY2005/06 = 307,986,014,000**, and the detector found **0** CapEx overlaps in those years (the only
  cross-listed lines were `econ2 "26 GRANTS"` + `add="capital"`, which **belong** to CapEx; the
  double-count is on the Other-grants side — [E5](#e5)).
- **⚠ FY2022/23–23/24: CapEx is NOT clean.** Its base `econ2` terms grab `add`-override lines that belong
  to Wage / Goods, producing the large **Wage ∩ CapEx (6.95T)** and **CapEx ∩ Goods (2.56T)** collisions —
  documented in **[E7](#e7)**. (This is why the earlier "no discrepancy" verdict, made when the detector
  only saw 2005/06, was wrong.) CapEx's `add="capital"` term is correct; the fix is on its `econ2` terms.

**Original (Excel), ≥FY2022/23 (`budget_type "<>03 External Financing"` added that era):**
```excel
=SUMIFS(executed,year,T$1,econ2,"31*",budget_type,"<>03 External Financing")
 +SUMIFS(executed,year,T$1,econ2,"23 CONSUMPTION OF FIXED ASSETS",budget_type,"<>03 External Financing")
 +SUMIFS(executed,year,T$1,add,"capital",budget_type,"<>03 External Financing")
```
**Proposed fix — the two `econ2` terms exclude the `add` overrides owned elsewhere (keep `add="capital"`):**
```excel
=SUMIFS(executed,year,T$1,econ2,"31*",budget_type,"<>03 External Financing",add,"<>wages",add,"<>nonwage")
 +SUMIFS(executed,year,T$1,econ2,"23 CONSUMPTION OF FIXED ASSETS",budget_type,"<>03 External Financing",add,"<>wages",add,"<>nonwage")
 +SUMIFS(executed,year,T$1,add,"capital",budget_type,"<>03 External Financing")
```
- **Added to both `econ2` terms:** `add,"<>wages",add,"<>nonwage"` (a `31*`/`23` line tagged `add="wages"`
  belongs to Wage, `add="nonwage"` to Goods). Apply the same to the early-era cells defensively.
- [ ] **Sign-off:** ____________________

<a id="e4"></a>
### E4 · Subsidies — disjointness criteria
- **Code** `EXP_ECON_SUB_EXE` · **Cell** `Executed!C15:U15` (label `B15`) · **Dim** econ · **Era** all years
- **Overlap found:** none in the data, but the criteria are needed so `assistance`/`pension`/`add`
  lines can never land here.

**Original (Excel):**
```excel
=SUMIFS(executed,year,C$1,econ2,"25 subsidies")
```
**Proposed (corrected):**
```excel
=SUMIFS(executed,year,C$1,econ2,"25 subsidies",pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage")
```
- [ ] **Sign-off:** ____________________

<a id="e5"></a>
### E5 · Other grants/transfers is over-counted (it double-counts Wage / Goods / CapEx `add` lines)
- **Code** `EXP_ECON_OTH_GRA_EXE` · **Cell** `Executed!C21:U21` (label `B21`) · **Dim** econ · **Era** all years
- **This is the econ category whose total is most wrong** and must come **down ≈1.94T** (full period).
  Wage, Goods and CapEx **own** the `add`-override lines and Social benefits owns the pension/assistance
  lines; all are unchanged — see [E1](#e1)–[E3](#e3).
- **Overlaps found (all 19 years):** ∩ CapEx **2,107 lines / 970B** (15 yrs) · ∩ Wage **1,699 / 263B**
  (16 yrs) · ∩ Goods **1,297 / 179B** (11 yrs) · ∩ Social benefits **8 / 524B** (FY2020/21–23/24).
- **Cause:** `econ2 "26 GRANTS"` lines that also carry an `add` override ("wages"/"capital"/"nonwage")
  are counted both here and in the override's (correct) category; the formula also fails to exclude
  `assistance`/`pension` lines (the 524B Social-benefits overlap).

**The formulas that count the same line:**
```excel
' >>> FIX THIS ONE  —  Other grants/transfers (Executed!C21)
=SUMIFS(executed,year,C$1,func1,"<>710 Social Protection",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264 To Resident Non-government units")

' ...also counted by  —  Wage / Goods / CapEx, each of which has an add-override term with NO econ2
'     filter, so an econ2 "26 GRANTS" line tagged add="wages"/"nonwage"/"capital" lands there too:
=SUMIFS(executed,year,C$1,add,"wages")     ' inside Wage bill            (Executed!C4)
=SUMIFS(executed,year,C$1,add,"nonwage")   ' inside Goods & services     (Executed!C11)
=SUMIFS(executed,year,C$1,add,"capital")   ' inside Capital expenditures (Executed!C8)
```
An `econ2 "26 GRANTS"` line carrying any `add` override is counted by **both Other grants/transfers and
the matching Wage / Goods / CapEx term** → double-counted (∩ CapEx 2,107/970B, ∩ Wage 1,699/263B,
∩ Goods 1,297/179B); pension/assistance lines with `econ2 "26"` are double-counted with Social benefits
(8/524B).

**Proposed fix — Other grants/transfers excludes the add-override & Social-benefit lines:**
```excel
=SUMIFS(executed,year,C$1,func1,"<>710 Social Protection",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264 To Resident Non-government units",pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage")
```
- **Added to Other grants/transfers:** `pension,"<>y"` · `assistance,"<>y"` · `add,"<>wages"` · `add,"<>capital"` · `add,"<>nonwage"`. The `add` lines stay only in their override category (Wage / Goods / CapEx).
- [ ] **Sign-off:** ____________________

<a id="e6"></a>
### E6 · Interest on debt — disjointness criteria
- **Code** `EXP_ECON_INT_DEB_EXE` · **Cell** `Executed!C26:U26` (label `B26`) · **Dim** econ · **Era** all years
- **Overlap found:** none in the data; defensive exclusion of `assistance`/`pension`.

**Original (Excel):**
```excel
=SUMIFS(executed,year,C$1,econ2,"24*")
```
**Proposed (corrected) — Rule 1 + Rule 2 (defensive; no overlap measured):**
```excel
=SUMIFS(executed,year,C$1,econ2,"24*",pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital",add,"<>nonwage")
```
- **Added:** `pension,"<>y",assistance,"<>y"` (Rule 1) · `add,"<>wages",add,"<>capital",add,"<>nonwage"` (Rule 2).
- [ ] **Sign-off:** ____________________

<a id="e7"></a>
### E7 · `add`-override collisions among Wage / Capital expenditures / Goods (FY2022/23–23/24) ⚠ largest econ overcount
- **Codes** `EXP_ECON_WAG_BIL_EXE` (`C4:U4`), `EXP_ECON_CAP_EXP_EXE` (`C8:U8`), `EXP_ECON_USE_GOO_SER_EXE` (`C11:U11`) · **Dim** econ · **Era** ≥ FY2022/23 (where the overlaps are measured)
- **Overlaps found:** **Wage ∩ Capital expenditures — 1,672 lines / 6.95T**; **Capital expenditures ∩ Goods — 3,281 lines / 2.56T** (both FY2022/23–23/24).
- **Cause:** each primary econ category is `SUMIFS(econ2 "<its code>") + SUMIFS(add,"<its override>")`.
  The **base `econ2` term has no `add` guard**, so it grabs lines whose `add` flag assigns them to a
  *different* category. A line with `econ2 "31*"` (CapEx) tagged `add="wages"` is counted by **CapEx's
  `econ2` term *and* Wage's `add` term**; `econ2 "21*"` + `add="capital"` is counted by **Wage's `econ2`
  term *and* CapEx's `add` term**; `econ2 "22…"`/`"31*"` with `add="capital"`/`"nonwage"` collide Goods↔CapEx.
- **Owner = the `add`-named category** (the override wins, per Rule 2); the *other* category's base
  `econ2` term is the over-count.

**The terms that collide (≥FY2022/23):**
```excel
' Wage bill (Executed!T4)          base econ2 term has NO add guard ─┐
=SUMIFS(executed,year,T$1,econ2,"21*",pension,"<>y")  +SUMIFS(executed,year,T$1,add,"wages")
' Capital expenditures (Executed!T8)                                │  collide on add-override lines
=SUMIFS(executed,year,T$1,econ2,"31*",budget_type,"<>03 External Financing") + … + SUMIFS(executed,year,T$1,add,"capital",…)
' Goods & services (Executed!T11)                                  ─┘
=SUMIFS(executed,year,T$1,econ2,"22 USE OF GOODS AND SERVICES")  +SUMIFS(executed,year,T$1,add,"nonwage")
```

**Proposed fix — add `add,"<>…"` guards to every *base `econ2`* term (each category keeps its own `add` term):**
```excel
' Wage bill  — econ2 term excludes the capital/nonwage overrides
=SUMIFS(executed,year,T$1,econ2,"21*",pension,"<>y",assistance,"<>y",add,"<>capital",add,"<>nonwage") +SUMIFS(executed,year,T$1,add,"wages",pension,"<>y",assistance,"<>y")
' Capital expenditures — both econ2 terms exclude the wages/nonwage overrides (see E3)
=…econ2,"31*",budget_type,"<>03 External Financing",add,"<>wages",add,"<>nonwage" + …econ2,"23…",…,add,"<>wages",add,"<>nonwage" + …add,"capital",…
' Goods & services — econ2 term excludes the wages/capital overrides
=SUMIFS(executed,year,T$1,econ2,"22 USE OF GOODS AND SERVICES",pension,"<>y",assistance,"<>y",add,"<>wages",add,"<>capital") +SUMIFS(executed,year,T$1,add,"nonwage",pension,"<>y",assistance,"<>y")
```
- This is the **same Rule 2** already applied to [E1](#e1)/[E2](#e2)/[E5](#e5); the pipeline (`econ`
  predicates `wage`/`capex`/`goods` with mutual `~addw/~addc/~addn` guards) already implements it, so the
  per-line tags are disjoint — only the **workbook codes** still double-count.
- ⚠ Why only FY2022/23–23/24? the `add` override column is densely populated from FY2022/23; in earlier
  years the same mechanism shows up smaller, against Other grants ([E5](#e5)).
- [ ] **Sign-off:** ____________________

> **`econ_sub`** overlaps share the `assistance,"<>y"` root cause: Allowances (`Executed!C6:U6`) ∩ Social
> Assistance (`Executed!C18:U18`) **14 lines / 84.8M** (8 yrs); Recurrent maintenance (`Executed!C14:U14`)
> ∩ Social Assistance **13 lines / 42.4M** (10 yrs). Allowances uses the year-union
> `econ5 "211103*" OR "211106*"` (the code changed at FY2022/23).

---

## 5. `func` issues

<a id="f1"></a>
### F1 · Health ∩ Education flag collision  ⚠ largest functional overcount
- **Codes** `EXP_FUNC_HEA_EXE` (`Executed!C215:U215`, label `B215`), `EXP_FUNC_EDU_EXE` (`Executed!C235:U235`, label `B235`) · **Dim** func · **Era** ≥ FY2022/23 only (fix lands in `T215:U215` / `T235:U235`)
- **Overlap found:** **301 lines / 127.6B** (FY2022/23–2023/24)
- **Cause:** from FY2022/23 the sector `func0 "12 Human Capital Development"` lumps health + education +
  social together (and `func1` is blank), so the Excel splits it with the **independent** flags
  `health,"y"` / `education,"y"` — 301 lines carry **both**.

**The two formulas that count the same line (≥FY2022/23):**
```excel
' Health (Executed!C215)
=SUMIFS(executed,year,C$1,func0,"08 health")          ' ≤FY2021/22
=SUMIFS(executed,year,T$1,health,"y")                 ' ≥FY2022/23

' Education (Executed!C235)
=SUMIFS(executed,year,C$1,func0,"07 Education")       ' ≤FY2021/22
=SUMIFS(executed,year,T$1,education,"y")              ' ≥FY2022/23
```
A `func0 "12 Human Capital Development"` line flagged **both** `health="y"` and `education="y"` is counted
by **both Health and Education** → double-counted (301 lines / 127.6B). (≤FY2021/22 the `func0` sectors are
mutually exclusive, so no overlap there.)

**Proposed (corrected)** — make the flags mutually exclusive (Health wins ties — see Q1):
```excel
' Health (≥FY2022/23): unchanged — claims all health-flagged lines
=SUMIFS(executed,year,T$1,health,"y")
' Education (≥FY2022/23): yields to Health
=SUMIFS(executed,year,T$1,education,"y",health,"<>y")
```
- **Added:** `health,"<>y"` on the Education flag term. **The real fix is at the flag source — see [H1](#h1).**
- ⚠ **Decision required (Q1):** which COFOG wins for a line flagged both? Pipeline default = **Health**.
- [ ] **Sign-off:** ____________________

<a id="f2"></a>
### F2 · Environmental protection (func, 705) claims Water & sanitation (func_sub of Housing, 706)
- **Code** `EXP_FUNC_ENV_PRO_EXE` · **Cell** `Executed!C193:U193` (label `B193`) — **also apply to the duplicate `EXP_FUNC_ENV_PRO_NEC_EXE` at `Executed!C199:U199`** · vs `EXP_FUNC_WAT_SAN_EXE` (`Executed!C205:U205`) · **Dim** func **×** func_sub (cross-level) · **Era** both
- **Overlap found:** **1,649 lines / 46.6B** (13 yrs) — the func-level **Environmental protection ∩
  Housing & community amenities** overlap (full 19-year coverage). It is driven by **water-supply lines
  (COFOG 706)** that Environment (705) double-counts; water & sanitation is a **`func_sub` of Housing**,
  not a top-level func. *(Earlier figures — "135.4B" then "42.3B" — were measured before the env-predicate
  fix and before shared-formula resolution gave full-year coverage; 46.6B is the current detector value.)*
- **Cause:** water-supply lines are tagged both **Environment** (705 / new sector "06 …Land And Water
  Management") and **Water & sanitation** (Housing 706). COFOG places water supply under **706**.
- ⚠ **Related finding (F2b — needs Massimo, see Q5):** the workbook's **Housing func total
  `EXP_FUNC_HOU_EXE` only composes water & sanitation in FY2005/06 and FY2022/23–23/24**; in
  **FY2006/07–2021/22 (16 yrs) water&san is absent from Housing** and sits inside Environment instead.
  So moving water out of Environment (the fix below) leaves it **homeless at the func level** in those
  16 years unless `EXP_FUNC_HOU_EXE` is *also* corrected to add the water&san term in every year
  (`+ C205`-style, as it already does for FY2005/06). Pipeline is unaffected — its `func` already tags
  every water line **Housing & community amenities** with `func_sub = Water and sanitation`.

**The two formulas that count the same line:**
```excel
' >>> FIX THIS ONE  —  Environmental protection (Executed!C193, and duplicate C199)
' ≤FY2021/22
=SUMIFS(executed,year,C$1,func1,"705 env*")
 +SUM(SUMIFS(executed,year,C$1,func1,"<>705 env*",Vote_Function,{"0906*","0908*","0904*","0905*","0907*","0951*"}))
' ≥FY2022/23
=SUMIFS(executed,year,T$1,func0,"06 Natural Resources, Environment, Climate Change, Land And Water Management",Vote_Function,"<>0602 Directorate of Water Resources Management",Vote_Function,"<>0602 Land, Administration and Management",Vote_Function,"<>0600 Unspecified ")

' ...also counted by  —  Water & sanitation (Housing 706) (Executed!C205) — the same water-supply lines:
' ≤FY2021/22
=SUM(SUMIFS(executed,year,C$1,Vote_Function,{"0901 Rural Water Supply and Sanitation","0902 Urban Water Supply and Sanitation","0981 Rural Water Supply and Sanitation","0982 Urban Water Supply and Sanitation"}))
' ≥FY2022/23
=SUM(SUMIFS(executed,year,T$1,wss,"y"))
```
A water-supply line with `func1 "705 env*"` (≤FY2021/22) or `wss="y"` (≥FY2022/23) is counted by **both
Environmental protection and Water & sanitation** → double-counted.

**Proposed fix — Environmental protection excludes the water & sanitation lines (they belong to Housing/706):**
```excel
' ≤FY2021/22: exclude the water-supply Vote_Functions from BOTH terms
=SUMIFS(executed,year,C$1,func1,"705 env*",Vote_Function,"<>0901*Water Supply and Sanitation",Vote_Function,"<>0902*Water Supply and Sanitation",Vote_Function,"<>0981*",Vote_Function,"<>0982*")
 +SUM(SUMIFS(executed,year,C$1,func1,"<>705 env*",Vote_Function,{"0906*","0908*","0904*","0905*","0907*","0951*"}))
' ≥FY2022/23: add wss,"<>y"  (note the "0600 Unspecified*" wildcard — see typo note below)
=SUMIFS(executed,year,T$1,func0,"06 Natural Resources, Environment, Climate Change, Land And Water Management",Vote_Function,"<>0602 Directorate of Water Resources Management",Vote_Function,"<>0602 Land, Administration and Management",Vote_Function,"<>0600 Unspecified*",wss,"<>y")
```
- **Added:** water-supply `Vote_Function` exclusions (≤FY2021/22) and `wss,"<>y"` (≥FY2022/23).
- **⚠ Trailing-space typo:** the workbook's existing exclusion `"<>0600 Unspecified "` has a **trailing
  space** and so (under Excel's literal text match) excludes **nothing** — the 0600 Unspecified lines
  (4.5B in FY2023/24) stay in Environment. Use the wildcard `"<>0600 Unspecified*"` so the exclusion
  actually fires regardless of spacing.
- **✅ Already in the pipeline (FY2022/23+):** `is_env` now excludes these three `Vote_Function`s and
  `wss="y"`; `is_hou` picks up `0602 Land, Administration and Management` → Housing (matching
  `EXP_FUNC_HOU_EXE`). Without this the transform over-counted Environmental protection by **~105B / 505
  lines** in FY2023/24. `0602 Directorate of Water Resources Management` (50.78B) and `0600 Unspecified`
  (4.5B) have no other func home → **General public services** (see [Q6](#q6)).
- ⚠ **Decision required (Q2):** confirm water supply = COFOG **706 Housing**, not 705.
- [ ] **Sign-off:** ____________________

<a id="f3"></a>
### F3 · Economic affairs ∩ Education (new broad sector set)
- **Code** `EXP_FUNC_ECO_REL_EXE` · **Cell** `Executed!T41:U41` (label `B41`; the ≤FY2021/22 cells `C41:S41` use the named-sector set and are unchanged) · **Dim** func · **Era** ≥ FY2022/23
- **Overlap found:** **14 lines / 0.40B** (FY2022/23)
- **Cause:** the new-coding Economic-affairs set `func0 {"01*"…"09*"}` overlaps lines that the
  `education` (and `health`) flags also claim.

**The two formulas that count the same line:**
```excel
' >>> FIX THIS ONE  —  Economic affairs (Executed!T41)
=SUM(SUMIFS(executed,year,T$1,func0,{"01*","02*","03*","04*","05*","07*","08*","09*"}))

' ...also counted by  —  Education (Executed!T235) — its flag grabs lines whose func0 is also in the set above
=SUMIFS(executed,year,T$1,education,"y")
```
A line with `func0 "07*"`/`"08*"` (in the Economic-affairs set) that also has `education="y"` is counted
by **both Economic affairs and Education** → double-counted.

**Proposed fix — Economic affairs excludes the Education/Health flagged lines:**
```excel
=SUM(SUMIFS(executed,year,T$1,func0,{"01*","02*","03*","04*","05*","07*","08*","09*"},education,"<>y",health,"<>y"))
```
- **Added to Economic affairs:** `education,"<>y"` · `health,"<>y"` (Health/Education own their flagged lines).
- [ ] **Sign-off:** ____________________

<a id="f4"></a>
### F4 · Social protection collides with six other functions ⚠ second-largest functional overcount
- **Code** `EXP_FUNC_SOC_PRO_EXE` · **Cell** `Executed!C257:U257` (label `B257`) · **Dim** func · **Era** all years
- **Formula (shared across C257:U257):** `=SUMIFS(executed,year,C$1,SP,"y")+SUMIFS(executed,year,C$1,pension,"y")` — Social protection is defined purely by the **`SP`/`pension` flags**, with **no exclusion** of any sector. So any line carrying `SP="y"` or `pension="y"` that *also* matches another function's criteria is double-counted.
- **What the flags carry — pensions are 90% of it.** `pension="y"` is **5.51T** (3,008 lines); `SP="y"`
  only **0.62T** (7,526 lines); together = the 6.13T Social-protection total (only 1 line carries both).
  The pension lines are explicit **retirement benefits** — `212102 Pension for General Civil Service`
  (1.60T), `1315 Public Service Pensions (Statutory)` (1.78T), `212104 Pension for Military Service`
  (0.77T), `212105 Pension for Local Governments` (0.69T), `212103 Pension for Teachers` (0.45T) — i.e.
  **COFOG 710 "old age."** They collide with a sector only because each pension line also carries the
  **paying ministry's** tag (teacher pensions in Vote 013 Education, military in Vote 004 Defence, …).
- **Overlaps found (full period — invisible before shared-formula resolution); ALL now resolved → Social protection:**

  | Partner function | lines | Σ executed | years |
  |---|--:|--:|--:|
  | Education | 63 | **200.6B** | 15 |
  | Public order & safety | 109 | **192.7B** | 10 |
  | Health | 416 | **183.2B** | 12 |
  | Economic affairs | 60 | **176.0B** | 13 |
  | Housing & community amenities | 38 | 19.5B | 12 |
  | Environmental protection | 12 | 9.6B | 6 |
  | Defence | 1 | 0 | 1 |

- **Cause:** the `SP`/`pension` flags sit on lines that also carry a sector tag (the ministry paying the
  pension). COFOG requires one function per line.
- **Resolution ([Q7](#q7) — pensions are COFOG 710):** Social protection **keeps** all `SP`/`pension`
  lines and **every sector excludes them**. The pipeline now makes Social protection the **top-priority**
  function (`p_socpro = is_socpro`; every other predicate carries `~is_socpro`) — verified disjoint
  (0 rows match >1) and reconciling **exactly** to the Excel `EXP_FUNC_SOC_PRO_EXE` value (e.g. FY2023/24
  = 430,013,365,677). This is the **opposite** of dropping the `pension` term, which would have stripped
  5.5T of pensions out of 710 and misattributed them to the paying sectors.

**Proposed Excel fix — each sector function adds `SP,"<>y", pension,"<>y"` (Social protection unchanged):**
```excel
' Add to EXP_FUNC_HEA_EXE, EXP_FUNC_EDU_EXE, EXP_FUNC_JUD_EXE, EXP_FUNC_PUB_SAF_EXE,
'        EXP_FUNC_DEF_EXE, EXP_FUNC_ECO_REL_EXE, EXP_FUNC_ENV_PRO_EXE, EXP_FUNC_HOU_EXE :
   …, SP,"<>y", pension,"<>y"
' EXP_FUNC_SOC_PRO_EXE is UNCHANGED:  =SUMIFS(…,SP,"y") + SUMIFS(…,pension,"y")
```
- **Effect (period totals move into 710):** Education −200.6B, Public order −192.7B, Health −183.2B,
  Economic affairs −176.0B, Housing −19.5B, Environment −9.6B. Social protection's total is unchanged
  (it already counted these lines; it now owns them exclusively).
- ⚠ **Massimo to confirm** the convention (civil-service / military / teacher pensions = **710**, not the
  paying sector). Closest precedent: **Bhutan**, a func-vs-func Social-protection overlap resolved by
  tightening the SP definition; the evidence here (items literally "Pension for …") points to 710.
- [ ] **Sign-off:** ____________________

---

## 6. `func_sub` — DEFERRED pending expert sign-off on the COFOG hierarchy

> ⚠ **The func/func_sub hierarchy is not yet agreed with the experts.** Until it is, the detector does
> **not** run overcount/overlap detection on `func_sub` (only `econ`, `econ_sub`, `func` are scanned —
> see `DIMENSIONS` in [UGA_detect_overcounting.py](UGA_detect_overcounting.py)). The table below is the
> *proposed* hierarchy resolution for discussion, **not** a confirmed finding. The pipeline still emits a
> `func_sub` tag per line, but no func_sub overlap figures are reported here.

<a id="fs1"></a>
### FS1 · Parent ⊃ child hierarchy — proposed resolution to most-specific (no Excel change)
- **Dim** func_sub · **proposed** (pending sign-off): treat these as parent/child rollups, not bugs. The
  per-line tag takes the **most specific** child; the Excel parent codes keep the rollup total.

| Parent ∩ child | Excel rows (parent / child) | lines | Σ executed | Per-line tag |
|---|---|--:|--:|---|
| Transport ⊃ Roads | `Executed!63` / `76` | 704 | 7.97T | **Roads** |
| Transport ⊃ Railroads | `Executed!63` / `91` | 2 | 292.4M | **Railroads** |
| Energy ⊃ Energy (power) | `Executed!130` / `143` | 954 | 2.82T | **Energy (power)** |
| Energy ⊃ Energy (oil & gas) | `Executed!130` / `155` | 639 | 1.31T | **Energy (oil & gas)** |

No `rail`/`air`/`water`-transport flag collisions were found (the flags are disjoint in practice).
- [ ] **Sign-off:** ____________________

---

## 7. Helper-flag formula fixes (root cause of [F1](#f1))

<a id="h1"></a>
### H1 · `health` / `education` flag definitions are not disjoint
- **Cell** `Expenditure!AE2:AE448145` (`health` flag, col **AE**) and `Expenditure!AF2:AF448145`
  (`education` flag, col **AF**) — these per-line `IF(SEARCH(...))` flag columns are the root cause of the
  [F1](#f1) collision. Make them mutually exclusive at source so no "12 Human Capital Development" line can
  be both:

**Proposed (Expenditure sheet, column AF `education`):**
```excel
=IF(AND(<existing education SEARCH test>, AE<row><>"y"), "y", "")   ' education yields to health (AE)
```
- Compute `health` (AE) first, then make `education` (AF) exclude it (or vice-versa per Q1). Also audit
  each flag's `SEARCH` strings against the FY2022/23 vote/program text for over-broad matches (the
  Questions-for-Massimo doc flags this pattern generically).
- [ ] **Sign-off:** ____________________

---

## 8. Subnational cross-code formula error (`EXP_CROSS_SBN_*`)

<a id="x1"></a>
### X1 · Subnational CapEx in water & sanitation — Excel array-broadcast double-count
- **Code** `EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE` · **Cell** `Executed!T278:U278` (label `B278`) · **Dim** cross-cutting (subnational × econ × func) · **Era** ≥ FY2022/23
- **Error found:** Excel cached **186,260,624,000** (FY22/23) and **208,808,794,896** (FY23/24) are **exactly 2×** the correct **93,130,312,000** / **104,404,397,448**. This is **not an overlap** between leaf categories — it is a computation bug *inside one cell's own formula*.
- **Cause — array→scalar broadcast.** `admin1,{"districts","Urban/Municipals"}` makes the **first** `SUMIFS` return a **2-element array**. The **second** `SUMIFS` is wrapped in its own `SUM(...)`, collapsing it to a **scalar**. Excel adds that scalar to *each* element of the 2-element array (broadcast) before the outer `SUM` totals them — so the "23 Consumption of fixed assets" term is added **twice**. (The `"31*"` term is 0 in the data, so the cached value is exactly 2× the `"23…"` term.)

**Original (Excel) — `Executed!T278` (≥FY2022/23):**
```excel
=SUM(SUMIFS(executed,admin1,{"districts","Urban/Municipals"},year,T$1,wss,"y",econ2,"31*")+SUM(SUMIFS(executed,admin1,{"districts","Urban/Municipals"},year,T$1,wss,"y",econ2,"23 CONSUMPTION OF FIXED ASSETS")))
```

**Proposed (corrected) — wrap *each* SUMIFS in its own `SUM()` before adding, so no array reaches the `+`:**
```excel
=SUM(SUMIFS(executed,admin1,{"districts","Urban/Municipals"},year,T$1,wss,"y",econ2,"31*"))+SUM(SUMIFS(executed,admin1,{"districts","Urban/Municipals"},year,T$1,wss,"y",econ2,"23 CONSUMPTION OF FIXED ASSETS"))
```
- **Changed:** moved the first `SUM(`'s closing `)` to immediately after the first `SUMIFS(...)`, so both terms are scalars when added. Reconciles to the detector value (93.13B / 104.40B).
- ⚠ **Same fragile shape — harden (latent, doubled term currently ≈ 0):** `EXP_CROSS_SBN_CAP_EXP_ENE_EXE` (`Executed!277`), `EXP_CROSS_SBN_CAP_EDU_EXE` (`Executed!272`), `EXP_CROSS_SBN_REC_EXP_ENE_EXE` (`Executed!280`) use the identical `array-SUMIFS + SUM(scalar)` pattern; they reconcile today only because the doubled term is ~0 in the validated years. Apply the same correction. (The same-shape-array codes `EXP_CROSS_CAP_EXP_WAT_SAN_EXE` and `EXP_CROSS_SBN_TRA_ROA_EXE` add two equal-length arrays and are **correct** — no change.)
- **Pipeline impact: none.** `EXP_CROSS_SBN_*` are reporting intersections, not leaf predicates of the `econ`/`func` partition, so no per-line tag changes.
- [ ] **Sign-off:** ____________________

> **Detector caveats now resolved.** A prior version of this section listed 7 `(code,year)` cells the
> detector could not reproduce. **Six were detector parser bugs**, since fixed in
> [UGA_detect_overcounting.py](UGA_detect_overcounting.py): **(a)** it trimmed cell values while Excel
> does not — restored to literal, case-insensitive matching with trailing spaces significant (fixes
> `EXP_FUNC_HOU_EXE` T/U and `EXP_FUNC_ENV_PRO_EXE`/`…_NEC` via the `"…Housing "` /
> `"<>0600 Unspecified "` criteria); **(b)** it ignored composite `+CELLREF` terms — now resolved
> recursively (fixes `EXP_FUNC_HOU_EXE` C = `SUMIFS(...)+C205` and `EXP_ECON_TOT_EXP_EXE` =
> `SUMIFS(...)−C25`); and **(c)** it read only `f.text`, missing Excel **shared formulas** (each row's
> formula lives on the col-C master; D:U inherit it) — now resolved with per-column reference shifting,
> which is what lifted coverage from 1,216 to **3,090** `(code,year)` cells across all 19 years. The
> detector now reproduces Excel on **3,088/3,090** literally and is parser-correct on **3,090/3,090**;
> only **X1** (a real Excel bug) remains, flagged in the detector's `EXCEL_FORMULA_ERRORS` registry.

---

## 9. Open questions for Massimo

1. **(Q1) Health vs Education tie** — rule for the 301 `func0 "12 Human Capital Development"` lines
   flagged both health and education. Pipeline default: **Health**. → [F1](#f1)/[H1](#h1)
2. **(Q2) Water-supply COFOG home** — confirm water & sanitation is **706 Housing**, not 705 Environment,
   so [F2](#f2) can exclude it from Environment.
3. **(Q3) `add` override authority** — confirm an `add` value ("wages"/"capital"/"nonwage") overrides the
   line's `econ2`-based class (so `econ2 26 GRANTS` + `add wages` → Wage, not Other grants). → [E1](#e1)–[E5](#e5)
4. **(Q4) Debt repayment** — `EXP_ECON_DEB_REP_EXE` (econ5 redemption codes) is excluded from the total;
   the pipeline drops these lines from `boost_gold`. Confirm.
5. **(Q5) Housing func total omits water & sanitation in FY2006/07–2021/22** — `EXP_FUNC_HOU_EXE`
   composes water&san only in FY2005/06 and FY2022/23–23/24 (see [F2b](#f2)). After Environmental
   protection drops the water lines, should `EXP_FUNC_HOU_EXE` add the water&san term in **all** years
   (so COFOG 706 Housing is complete), or does water&san roll up to the func level some other way in
   those years? Confirms the destination for the 42.3B moved out of Environment.
<a id="q6"></a>
6. **(Q6) "Directorate of Water Resources Management" has no functional home** — the env formula (and
   the pipeline) excludes `Vote_Function "0602 Directorate of Water Resources Management"` (**50.78B in
   FY2023/24**) from Environmental protection, and no other `EXP_FUNC_*` code claims it, so it currently
   falls to **General public services**. Functionally this looks like it should be **Environmental
   protection** or **Water & sanitation** — confirm the intended COFOG home. (See [F2](#f2).)
<a id="q7"></a>
7. **(Q7) Social protection vs sector** — `SP`/`pension` lines (5.5T, mostly civil-service / military /
   teacher / local-gov **pension benefits**) also carry the paying ministry's sector tag, so they're
   double-counted ([F4](#f4)). **Chosen resolution (implemented):** pensions are **COFOG 710** — Social
   protection keeps them and **every sector excludes `SP`/`pension`**; SP total then matches the Excel
   `EXP_FUNC_SOC_PRO_EXE` code exactly. **Massimo to confirm** that civil-service/military/teacher pensions
   belong in 710 rather than the paying sector (the items are literally "Pension for …", which supports 710).

---

## 10. Sign-off

| Reviewer | Role | Decision | Date |
|---|---|---|---|
|  |  | ☐ approved ☐ changes requested |  |
