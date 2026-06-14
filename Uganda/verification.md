# Uganda BOOST — overcounting review & sign-off

- **What this is:** every overcounting overlap found onboarding Uganda, **organized by pair** — both colliding categories' current formulas side by side (each cell names its category), the overlap size, the lines they share, and a proposed fix. **No precedence is assumed:** the proposed fix is shown *under the suggested default* — the expert decides which category owns the shared lines, and the exclusion then sits on the **other** side (or propose another split).
- **How to read a fix:** "X += `…,"<>…"`" means add that exclusion to category X's formula so the shared lines stay only with the owner. Defaults are in [Decisions](#decisions) and coded in [UGA_transform_load_dlt.py](UGA_transform_load_dlt.py) — flip any of them and the fix moves to the other category.
- **Validation:** the detector reproduces the workbook's cached totals for **3,088 / 3,090** `(code, year)` cells across all 19 years; the 2 exceptions are [X1](#x1). Magnitudes measured on the full microdata — see [_detect/overcounting_report.md](_detect/overcounting_report.md).
- **Reading:** every formula is `SUMIFS(executed, year, T$1, …)` (≥FY2022/23 cells) — criteria only, `+` joins additive SUMIFS terms, `bt` = `budget_type,"<>03 External Financing"`.

## `econ` — overlaps by pair

| Overlap pair · overlap | **Category A** (formula) | **Category B** (formula) | Shared lines (counted by both) | Proposed fix (under default) |
|---|---|---|---|---|
| **Wage ∩ Social benefits** · 7.2B | **A = Wage** `C4` — `econ2,"21*",pension,"<>y"` + `add,"wages"` | **B = Social benefits** `C18,C19` — `assistance,"y"` + `pension,"y"` | `assistance`/`pension` lines also matched by a Wage term (3.3B via `econ2 "21"`, 3.9B via `add "wages"`) | B owns → **Wage** both terms += `pension,"<>y",assistance,"<>y"` |
| **Goods ∩ Social benefits** · 12.3B | **A = Goods** `C11` — `econ2,"22 USE OF GOODS AND SERVICES"` + `add,"nonwage"` | **B = Social benefits** `C18,C19` — `assistance,"y"` + `pension,"y"` | `assistance`/`pension` lines also matched by a Goods term (3.5B `econ2 "22"`, 8.8B `add "nonwage"`) | B owns → **Goods** both terms += `pension,"<>y",assistance,"<>y"` |
| **Other grants ∩ Social benefits** · 524B | **A = Other grants** `C21` — `func1,"<>710 Social Protection",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264 To Resident Non-government units"` | **B = Social benefits** `C18,C19` — `assistance,"y"` + `pension,"y"` | `econ2 "26"` grant lines that are `assistance`/`pension` | B owns → **Other grants** += `pension,"<>y",assistance,"<>y"` |
| **Capital exp ∩ Wage** · 6.95T | **A = Capital exp** `C8` — `econ2,"31*",bt` + `econ2,"23 CONSUMPTION OF FIXED ASSETS",bt` + `add,"capital",bt` | **B = Wage** `C4` — `econ2,"21*",pension,"<>y"` + `add,"wages"` | `econ2 "23"/"31"` lines tagged `add="wages"` (A's `econ2` term vs B's `add` term) | B owns (`add`) → **Capital exp** `econ2` terms += `add,"<>wages"` |
| **Capital exp ∩ Goods** · 2.56T | **A = Capital exp** `C8` — `econ2,"31*",bt` + `econ2,"23 CONSUMPTION OF FIXED ASSETS",bt` + `add,"capital",bt` | **B = Goods** `C11` — `econ2,"22 USE OF GOODS AND SERVICES"` + `add,"nonwage"` | `econ2 "23"/"31"` lines tagged `add="nonwage"` (A's `econ2` term vs B's `add` term) | B owns (`add`) → **Capital exp** `econ2` terms += `add,"<>nonwage"` |
| **Other grants ∩ Capital exp** · 970B | **A = Other grants** `C21` — `func1,"<>710…",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264…"` | **B = Capital exp** `C8` — `add,"capital",bt` term | `econ2 "26"` grant lines tagged `add="capital"` | B owns (`add`) → **Other grants** += `add,"<>capital"` |
| **Other grants ∩ Wage** · 263B | **A = Other grants** `C21` — `func1,"<>710…",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264…"` | **B = Wage** `C4` — `add,"wages"` term | `econ2 "26"` grant lines tagged `add="wages"` | B owns (`add`) → **Other grants** += `add,"<>wages"` |
| **Other grants ∩ Goods** · 179B | **A = Other grants** `C21` — `func1,"<>710…",econ2,"26 GRANTS",transfer,"<>1",econ3,"<264…"` | **B = Goods** `C11` — `add,"nonwage"` term | `econ2 "26"` grant lines tagged `add="nonwage"` | B owns (`add`) → **Other grants** += `add,"<>nonwage"` |

_Discriminators available to separate any pair: `pension`/`assistance` flags, the `add` value, `econ2`. No overlaps in Subsidies/Interest; econ_sub Allowances/Recurrent-maint ∩ Social Assistance = 84.8M+42.4M. Capital exp also double-counts **within its own formula** — separate issue, see [§ Within-category double-count](#self)._

## `func` — overlaps by pair

| Overlap pair · overlap · Q | **Category A** (formula) | **Category B** (formula) | Shared lines (counted by both) | Proposed fix (under default) |
|---|---|---|---|---|
| **Education ∩ Social protection** · 200.6B · Q7 | **A = Education** `C235` — `education,"y"` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also `education="y"` | B owns (710) → **Education** += `SP,"<>y",pension,"<>y"` |
| **Public order ∩ Social protection** · 192.7B · Q7 | **A = Public order** `C32,C36` — `admin2,{"101 judiciary*"}` + `security,"y"` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also Judiciary / `security` | B owns (710) → **Public order** += `SP,"<>y",pension,"<>y"` |
| **Health ∩ Social protection** · 183.2B · Q7 | **A = Health** `C215` — `health,"y"` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also `health="y"` | B owns (710) → **Health** += `SP,"<>y",pension,"<>y"` |
| **Economic affairs ∩ Social protection** · 176.0B · Q7 | **A = Economic affairs** `C41` — `func0,{"01*","02*","03*","04*","05*","07*","08*","09*"}` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also in the Economic-affairs `func0` set | B owns (710) → **Economic affairs** += `SP,"<>y",pension,"<>y"` |
| **Housing ∩ Social protection** · 19.5B · Q7 | **A = Housing** `C203` — `func0,"10 Sustainable Urbanisation And Housing "` + `wss,"y"` + `Vote_Function,{"0612…","0602 Land…"}` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also Housing | B owns (710) → **Housing** += `SP,"<>y",pension,"<>y"` (each term) |
| **Environmental protection ∩ Social protection** · 9.6B · Q7 | **A = Env** `C193,C199` — `func0,"06 Natural Resources, Environment, Climate Change, Land And Water Management",Vote_Function,"<>0602…",Vote_Function,"<>0602 Land…",Vote_Function,"<>0600 Unspecified "` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also Environment | B owns (710) → **Env** += `SP,"<>y",pension,"<>y"` |
| **Defence ∩ Social protection** · ~0 · Q7 | **A = Defence** `C28` — `Vote_Function,"1601 National Defence (UPDF)"` | **B = Social protection** `C257` — `SP,"y"` + `pension,"y"` | `SP`/`pension` lines also Defence | B owns (710) → **Defence** += `SP,"<>y",pension,"<>y"` |
| **Health ∩ Education** (flag tie) · 127.6B · Q1 | **A = Health** `C215` — `health,"y"` | **B = Education** `C235` — `education,"y"` | lines flagged **both** `health="y"` and `education="y"` | A owns (default) → **Education** += `health,"<>y"` |
| **Environmental protection ∩ Housing** (water) · 46.6B · Q2 | **A = Env** `C193,C199` — `func0,"06 Natural Resources…",Vote_Function,"<>0602…",Vote_Function,"<>0602 Land…",Vote_Function,"<>0600 Unspecified "` | **B = Housing** `C203` — `func0,"10 Sustainable Urbanisation And Housing "` + `wss,"y"` + `Vote_Function,{"0612…","0602 Land…"}` | water-supply lines (`wss` / water `Vote_Function`) claimed by both 705 (A) and 706 (B) | B owns (706) → **Env** += `wss,"<>y"` (+ fix typo `"<>0600 Unspecified*"`) |
| **Economic affairs ∩ Education** · 0.40B | **A = Economic affairs** `C41` — `func0,{"01*","02*",…,"09*"}` | **B = Education** `C235` — `education,"y"` | Economic-affairs `func0` lines also `education="y"` | B owns → **Economic affairs** += `education,"<>y",health,"<>y"` |

_Discriminators to separate any pair: the `SP`/`pension`/`health`/`education`/`security`/`wss` flags, `func0`, `Vote_Function`. Env's `"<>0600 Unspecified "` has a trailing-space typo (matches nothing) — should be `"<>0600 Unspecified*"`. func_sub overlaps not reported (hierarchy not yet agreed; deferred)._

<a id="self"></a>
## Within-category double-count — Capital expenditures (not a pair, not a precedence question)

A single category's own `SUMIFS` terms overlap, so the **same line is counted twice inside one formula**. Not a precedence call — the formula just needs to count each line once. (Invisible to validation: Excel and the parsed formula sum the terms the same way.)

- **Capital exp** `Executed!C8:U8` · **2.32T** (FY22/23–23/24): its `econ2 "31*"/"23"` terms **and** its own `add,"capital"` term both match `econ2 "23"/"31"` lines tagged `add="capital"`.
- Same shape in reporting codes `EXP_CROSS_CAP_EXP_EDU_EXE` (698B) and `EXP_CROSS_CAP_EXP_TRA_EXE` (238B) — fix their `econ2` terms the same way.

| Current (Capital exp) | Proposed fix |
|---|---|
| `econ2,"31*",bt` + `econ2,"23 CONSUMPTION OF FIXED ASSETS",bt` + `add,"capital",bt` | both `econ2` terms += `add,"<>capital"` (keep the `add,"capital"` term) → each line counted once |

<a id="x1"></a>
## X1 — confirmed Excel formula bug (not a precedence question — the cell is simply wrong)

`EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE` (`Executed!T278:U278`) **double-counts via array broadcast** — cached is **2× correct** (186.3B vs 93.1B FY22/23; 208.8B vs 104.4B FY23/24). Same fragile shape (latent today): rows 272, 277, 280.

| Current | Proposed fix |
|---|---|
| `SUM(SUMIFS(…,admin1,{"districts","Urban/Municipals"},…,econ2,"31*")+SUM(SUMIFS(…,econ2,"23 CONSUMPTION OF FIXED ASSETS")))` | `SUM(SUMIFS(…,econ2,"31*")) + SUM(SUMIFS(…,econ2,"23 CONSUMPTION OF FIXED ASSETS"))` — wrap *each* SUMIFS in its own `SUM()` |

<a id="decisions"></a>
## Decisions for Massimo (precedence per pair — change freely; the proposed fix above flips with these)

| # | Decision | Pipeline default (suggestion) |
|---|---|---|
| **Q1** | Health vs Education tie — 301 lines / 127.6B flagged both | Health owns |
| **Q2** | Water & sanitation lines — 705 Environment or 706 Housing? | 706 Housing owns |
| **Q3** | When a line carries an `add` override (`wages`/`capital`/`nonwage`) **and** an `econ2` class **and**/or a Social-benefit flag — which wins? | `add` beats `econ2`; Social-benefit beats `add` |
| **Q4** | Debt-repayment `econ5` codes — exclude from the total? (pipeline drops them) | excluded |
| **Q5** | `EXP_FUNC_HOU_EXE` includes water&san only in FY05/06 & FY22/23–23/24 — add it in **all** years? | (open) |
| **Q6** | "Directorate of Water Resources Management" (50.8B) — Env, Water&san, or General public services? | (currently falls to GPS) |
| **Q7** | Civil-service / military / teacher **pensions (5.5T)** — Social protection 710, or the paying sector? | 710 owns |

## Sign-off

| Reviewer | Decision | Date |
|---|---|---|
|  | ☐ approved  ☐ changes requested |  |
