# Moldova BOOST — Expert Verification

This document lists every item in the Moldova BOOST workbook that needs subject-matter-expert confirmation before we publish the pipeline output. Two classes of issues are covered:

1. **Formula overcounting** — rows double-counted by multiple codes that are meant to be mutually exclusive.
2. **Hard-coded overrides** — cells where a literal value replaces the formula, so the pipeline can't reproduce it.

Each item shows the current Excel behaviour, the severity, and a proposed fix following the cross-country conventions from previous fixes (more-specific SUMIFS wins; rename or drop duplicates; recover overrides as explicit formulas). The sign-off checklist at the bottom captures accept/reject per item — once signed, we update the workbook and the pipeline reruns clean.

**At a glance**

- Overcounting pairs: **7** (5 cross-category, 2 same-value duplicate)
- Hard-coded overrides: **28** cells across 2 code(s)

## A. Formula overcounting

Rows tagged by multiple codes that are meant to be mutually exclusive. The _Overcounted_ column is the sum of the raw measure (approved / executed) over rows that match **both** codes' SUMIFS filters — the exact amount that is double-counted when the two codes are summed together. Severity is `Overcounted ÷ min(Σ code A, Σ code B)` — a 100% figure means one code's total is wholly contained in the other's.

### Range 2006-2015

#### A1. `EXP_ECON_GOO_SER_EMP_CON_EXE` × `EXP_ECON_SOC_ASS_EXE` — **Low** (cross-category)

- Shared level: `econ_sub:cross[Employment contracts|Social assistance]`
- Intersection: 1 raw rows · Σ approved **12,100** · Σ executed **0**
- Severity ratio (overcounted ÷ min total): approved 0.0% · executed 0.0%

**Diagnosis.** Social assistance (`func1=10 Social care…`) is broad; Employment contracts (`econ2=113.16 Research and innovation services contracted out…`) is narrower. The narrower code should win.

**Proposed fix.**

Exclude employment-contracts rows from Social assistance:
`=SUMIFS(executed,year,C$1,func1,"10 Social care and social insurance",transfer,"Excluding transfers",econ2,"<>113.16*") - C19`

#### A2. `EXP_ECON_REC_MAI_EXE` × `EXP_ECON_SOC_ASS_EXE` — **Medium** (cross-category)

- Shared level: `econ_sub:cross[Recurrent maintenance|Social assistance]`
- Intersection: 74 raw rows · Σ approved **8,837,014** · Σ executed **16,560,658**
- Severity ratio (overcounted ÷ min total): approved 4.7% · executed 4.6%

**Diagnosis.** Recurrent maintenance (`econ2=113.18 Current repair…`) is narrower than Social assistance (`func1=10`). The narrower code should win.

**Proposed fix.**

Exclude recurrent-maintenance rows from Social assistance:
`=SUMIFS(executed,year,C$1,func1,"10 Social care and social insurance",transfer,"Excluding transfers",econ2,"<>113.18*") - C19`

#### A3. `EXP_ECON_SOC_ASS_EXE` × `EXP_ECON_SUB_PRO_EXE` — **Low** (cross-category)

- Shared level: `econ_sub:cross[Social assistance|Subsidies to production]`
- Intersection: 6 raw rows · Σ approved **710,000** · Σ executed **267,100**
- Severity ratio (overcounted ÷ min total): approved 0.0% · executed 0.0%

**Diagnosis.** Subsidies to production (`econ1=132 Transfers for production`) is narrower than Social assistance (`func1=10`). The narrower wins.

**Proposed fix.**

Exclude subsidies-to-production rows from Social assistance:
`=SUMIFS(executed,year,C$1,func1,"10 Social care and social insurance",transfer,"Excluding transfers",econ1,"<>132*") - C19`

#### A4. `EXP_FUNC_PRI_EDU_EXE` × `EXP_FUNC_PRI_SEC_EDU_EXE` — **Critical** (cross-category)

- Shared level: `func_sub:cross[Primary education|Primary and secondary education]`
- Intersection: 984 raw rows · Σ approved **13,879,192,390** · Σ executed **14,566,709,920**
- Severity ratio (overcounted ÷ min total): approved 100.0% · executed 100.0%

**Diagnosis.** `EXP_FUNC_PRI_SEC_EDU_EXE` is a **rollup** that sums Preschool + Primary + Secondary. `EXP_FUNC_PRI_EDU_EXE` (Preschool + Primary) is wholly inside it, so 100% of PRI lives in PRI_SEC.

**Proposed fix.**

Drop `EXP_FUNC_PRI_SEC_EDU_EXE` from the econ_sub breakdown — it is a derived total, not a leaf sub-category. Keep `EXP_FUNC_PRI_EDU_EXE` and `EXP_FUNC_SEC_EDU_EXE` as the two disjoint leaves.

#### A5. `EXP_FUNC_PRI_SEC_EDU_EXE` × `EXP_FUNC_SEC_EDU_EXE` — **Critical** (cross-category)

- Shared level: `func_sub:cross[Primary and secondary education|Secondary education]`
- Intersection: 1,072 raw rows · Σ approved **31,697,013,847** · Σ executed **32,072,715,339**
- Severity ratio (overcounted ÷ min total): approved 100.0% · executed 100.0%

**Diagnosis.** Same rollup issue: `EXP_FUNC_PRI_SEC_EDU_EXE` contains `EXP_FUNC_SEC_EDU_EXE` in full (100% overlap).

**Proposed fix.**

Same action: drop `EXP_FUNC_PRI_SEC_EDU_EXE` as a reported sub-category.

### Range 2016-2019

#### A6. `REV_ECON_CUS_EXC_EXE` × `REV_ECON_EXC_EXE` — **Critical** (same-value duplicate)

- Shared level: `econ:Customs and excise=Customs and excise`
- Intersection: 124 raw rows · Σ approved **22,013,112,320** · Σ executed **22,401,756,678**
- Severity ratio (overcounted ÷ min total): approved 100.0% · executed 100.0%

**Diagnosis.** `REV_ECON_CUS_EXC_EXE` and `REV_ECON_EXC_EXE` resolve to the **identical** SUMIFS (both filter `econ4=114200 Accize`). The label *Customs/excise* implies Customs should also be included; the current formula only captures Excises.

**Proposed fix.**

Option 1 — fix the Customs/excise formula to actually include customs:
`=SUMIFS(approved_16,year_16,M$1,transfer_16,"Cu exceptia transferurilor",econ0_16,"Revenues",econ4_16,{"114200 Accize","114100 Taxe vamale"})`
Option 2 — drop one of the two codes as redundant.

### Range 2020-2024

#### A7. `REV_ECON_CUS_EXC_EXE` × `REV_ECON_EXC_EXE` — **Critical** (same-value duplicate)

- Shared level: `econ:Customs and excise=Customs and excise`
- Intersection: 93 raw rows · Σ approved **41,805,213,800** · Σ executed **43,675,341,750**
- Severity ratio (overcounted ÷ min total): approved 100.0% · executed 100.0%

**Diagnosis.** `REV_ECON_CUS_EXC_EXE` and `REV_ECON_EXC_EXE` resolve to the **identical** SUMIFS (both filter `econ4=114200 Accize`). The label *Customs/excise* implies Customs should also be included; the current formula only captures Excises.

**Proposed fix.**

Option 1 — fix the Customs/excise formula to actually include customs:
`=SUMIFS(approved_16,year_16,M$1,transfer_16,"Cu exceptia transferurilor",econ0_16,"Revenues",econ4_16,{"114200 Accize","114100 Taxe vamale"})`
Option 2 — drop one of the two codes as redundant.

## B. Hard-coded overrides

**28 cells** in Approved / Executed contain a typed numeric literal instead of a SUMIFS formula. The pipeline reproduces SUMIFS results only, so each override below will surface as a per-run discrepancy. Proposed fix (cross-country pattern): recover each override as an explicit formula — usually a SUMIFS minus a specific sibling row, or a SUMIFS over a different filter — so the calculation is reproducible and auditable.

### B1. `EXP_ECON_SOC_ASS_EXE` — Social Assistance

- Sheets: Approved, Executed · Years: 2009–2015 · 14 cells total

| Sheet | Cell | Year | Value |
|---|---|---:|---:|
| Approved | F18 | 2009 | 1,590,600,000 |
| Approved | G18 | 2010 | 2,005,400,000 |
| Approved | H18 | 2011 | 2,248,400,000 |
| Approved | I18 | 2012 | 455,900,000 |
| Approved | J18 | 2013 | 485,800,000 |
| Approved | K18 | 2014 | 3,018,300,000 |
| Approved | L18 | 2015 | 3,789,400,000 |
| Executed | F18 | 2009 | 1,590,600,000 |
| Executed | G18 | 2010 | 2,005,400,000 |
| Executed | H18 | 2011 | 2,248,400,000 |
| Executed | I18 | 2012 | 455,900,000 |
| Executed | J18 | 2013 | 485,800,000 |
| Executed | K18 | 2014 | 3,018,300,000 |
| Executed | L18 | 2015 | 3,789,400,000 |

**Proposed fix.** Ask the SME to supply the SUMIFS expression (or SUMIFS − sibling-cell difference) that produced each value, then replace the literal in the workbook. Once the Excel is reformulated, the pipeline will reproduce it automatically.

### B2. `EXP_ECON_SOC_BEN_PEN_EXE` — Pensions

- Sheets: Approved, Executed · Years: 2009–2015 · 14 cells total

| Sheet | Cell | Year | Value |
|---|---|---:|---:|
| Approved | F19 | 2009 | 7,100,700,000 |
| Approved | G19 | 2010 | 7,696,400,000 |
| Approved | H19 | 2011 | 8,237,800,000 |
| Approved | I19 | 2012 | 10,515,100,000 |
| Approved | J19 | 2013 | 11,514,500,000 |
| Approved | K19 | 2014 | 10,370,900,000 |
| Approved | L19 | 2015 | 11,159,100,000 |
| Executed | F19 | 2009 | 7,100,700,000 |
| Executed | G19 | 2010 | 7,696,400,000 |
| Executed | H19 | 2011 | 8,237,800,000 |
| Executed | I19 | 2012 | 10,515,100,000 |
| Executed | J19 | 2013 | 11,514,500,000 |
| Executed | K19 | 2014 | 10,370,900,000 |
| Executed | L19 | 2015 | 11,159,100,000 |

**Proposed fix.** Ask the SME to supply the SUMIFS expression (or SUMIFS − sibling-cell difference) that produced each value, then replace the literal in the workbook. Once the Excel is reformulated, the pipeline will reproduce it automatically.

## C. Sign-off checklist

One checkbox per item. Tick _Accept_ if the proposed fix should be applied to the workbook; tick _Reject_ with a note if the current behaviour is correct as-is.

### A. Overcounting

- [ ] **A1** `EXP_ECON_GOO_SER_EMP_CON_EXE` × `EXP_ECON_SOC_ASS_EXE` (2006-2015) — accept fix / reject (note: _______)
- [ ] **A2** `EXP_ECON_REC_MAI_EXE` × `EXP_ECON_SOC_ASS_EXE` (2006-2015) — accept fix / reject (note: _______)
- [ ] **A3** `EXP_ECON_SOC_ASS_EXE` × `EXP_ECON_SUB_PRO_EXE` (2006-2015) — accept fix / reject (note: _______)
- [ ] **A4** `EXP_FUNC_PRI_EDU_EXE` × `EXP_FUNC_PRI_SEC_EDU_EXE` (2006-2015) — accept fix / reject (note: _______)
- [ ] **A5** `EXP_FUNC_PRI_SEC_EDU_EXE` × `EXP_FUNC_SEC_EDU_EXE` (2006-2015) — accept fix / reject (note: _______)
- [ ] **A6** `REV_ECON_CUS_EXC_EXE` × `REV_ECON_EXC_EXE` (2016-2019) — accept fix / reject (note: _______)
- [ ] **A7** `REV_ECON_CUS_EXC_EXE` × `REV_ECON_EXC_EXE` (2020-2024) — accept fix / reject (note: _______)

### B. Hard-coded overrides

- [ ] **B1** `EXP_ECON_SOC_ASS_EXE` (14 cells) — SME to supply formulas / accept literals
- [ ] **B2** `EXP_ECON_SOC_BEN_PEN_EXE` (14 cells) — SME to supply formulas / accept literals

### Final

- [ ] Reviewer: __________________________
- [ ] Date: __________________________
- [ ] Workbook updated; rerun `python3 _onboarding/scripts/6_detect_overlaps.py` and `python3 _onboarding/scripts/9_build_verification_doc.py` to confirm resolved items drop out.
