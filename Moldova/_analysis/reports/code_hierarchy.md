# Moldova BOOST — code-dictionary hierarchy for SME review

Every non-CROSS TAG code mapped to canonical `econ` / `econ_sub` / `func` / `func_sub` labels shared with the other countries in the cross-country aggregate. Review the groupings below and flag anything that should land under a different parent, plus any code flagged in the _Notes_ column.

- Non-CROSS TAG codes in dictionary: **62**
- `EXP_CROSS_*` codes excluded (double-count with EXP_ECON×EXP_FUNC parents): **86**
- Meta-rollup codes excluded (EXP_ECON_SBN_TOT_SPE_EXE, EXP_ECON_TOT_EXP_EXE, EXP_FUNC_ECO_REL_EXE, REV_ECON_TOT_EXE): **4**
- Codes with a flagged `decompose_notes` entry: **7**


## Expenditure — Economic classification (EXP_ECON)

### Capital expenditures — 3 code(s)
  - `EXP_ECON_CAP_EXP_EXE` — Spending: Capital Expenditures
  - `EXP_ECON_SBN_CAP_SPE_EXE` — Subnational: Capital spending _⚠ Moldova subnational tag — confirm admin0/geo0 handling with SME_
  - **Capital maintenance**
    - `EXP_ECON_CAP_MAI_EXE` — Spending in capital maintenance

### Goods and services — 5 code(s)
  - `EXP_ECON_SBN_REC_SPE_EXE` — Subnational: Recurrent spending _⚠ Moldova subnational tag — confirm admin0/geo0 handling with SME_
  - `EXP_ECON_USE_GOO_SER_EXE` — Spending: Use of Goods and services
  - **Basic services**
    - `EXP_ECON_GOO_SER_BAS_SER_EXE` — Spending in Goods and services (basic services)
  - **Employment contracts**
    - `EXP_ECON_GOO_SER_EMP_CON_EXE` — Spending in Goods and services (employment contracts)
  - **Recurrent maintenance**
    - `EXP_ECON_REC_MAI_EXE` — Spending in recurrent maintenance

### Interest on debt — 1 code(s)
  - `EXP_ECON_INT_DEB_EXE` — Spending: Interest on debt

### Other expenses — 1 code(s)
  - `EXP_ECON_OTH_MED_RIG_EXP_EXE` — Other medium rigidity expenditures

### Other grants and transfers — 1 code(s)
  - `EXP_ECON_SBN_TOT_TRA_EXE` — Subnational: Total transfers _⚠ Moldova subnational tag — confirm admin0/geo0 handling with SME_

### Social benefits — 3 code(s)
  - **Pension contributions**
    - `EXP_ECON_PEN_CON_EXE` — Social benefits - (pension contributions)
  - **Pensions**
    - `EXP_ECON_SOC_BEN_PEN_EXE` — Pensions
  - **Social assistance**
    - `EXP_ECON_SOC_ASS_EXE` — Social Assistance

### Subsidies — 2 code(s)
  - `EXP_ECON_SUB_EXE` — Spending in subsidies
  - **Subsidies to production**
    - `EXP_ECON_SUB_PRO_EXE` — Spending: Subsidies to production

### Wage bill — 1 code(s)
  - `EXP_ECON_WAG_BIL_EXE` — Spending: Wage bill


## Expenditure — Functional classification (EXP_FUNC)

### Defence — 1 code(s)
  - `EXP_FUNC_DEF_EXE` — Defense (COFOG 702)

### Economic affairs — 11 code(s)
  - `EXP_FUNC_ENE_EXE` — Spending in energy
  - `EXP_FUNC_TRA_EXE` — Spending in transport
  - **Agriculture**
    - `EXP_FUNC_AGR_EXE` — Spending in agriculture
    - `EXP_FUNC_SBN_AGR_EXE` — Subnational: Spending in agriculture _⚠ Moldova subnational tag — confirm admin0/geo0 handling with SME_
  - **Energy (heating)**
    - `EXP_FUNC_ENE_HEA_EXE` — Spending in energy (heating)
  - **Energy (oil & gas)**
    - `EXP_FUNC_ENE_OIL_EXE` — Spending in energy (oil & gas)
  - **Energy (power)**
    - `EXP_FUNC_ENE_POW_EXE` — Spending in energy (power)
  - **Irrigation**
    - `EXP_FUNC_IRR_EXE` — Spending in Irrigation
  - **Roads**
    - `EXP_FUNC_ROA_EXE` — Spending in roads
  - **Telecom**
    - `EXP_FUNC_TEL_EXE` — Spending in telecoms
  - **Water transport**
    - `EXP_FUNC_WAT_TRA_EXE` — Spending in water transport

### Education — 6 code(s)
  - `EXP_FUNC_EDU_EXE` — Education (COFOG 709)
  - `EXP_FUNC_SBN_EDU_EXE` — Subnational: Spending in education _⚠ Moldova subnational tag — confirm admin0/geo0 handling with SME_
  - **Primary and secondary education**
    - `EXP_FUNC_PRI_SEC_EDU_EXE` — Spending in primary and secondary education
  - **Primary education**
    - `EXP_FUNC_PRI_EDU_EXE` — Spending in primary education
  - **Secondary education**
    - `EXP_FUNC_SEC_EDU_EXE` — Spending in secondary education
  - **Tertiary education**
    - `EXP_FUNC_TER_EDU_EXE` — Spending in tertiary education

### Environmental protection — 2 code(s)
  - `EXP_FUNC_ENV_PRO_EXE` — Environment Protection (COFOG 705)
  - **N.E.C**
    - `EXP_FUNC_ENV_PRO_NEC_EXE` — Environmental Protection N.E.C

### General public services — 1 code(s)
  - **Judiciary**
    - `EXP_FUNC_JUD_EXE` — Spending in judiciary

### Health — 2 code(s)
  - `EXP_FUNC_HEA_EXE` — Health (COFOG 707)
  - `EXP_FUNC_SBN_HEA_EXE` — Subnational: Spending in health _⚠ Moldova subnational tag — confirm admin0/geo0 handling with SME_

### Housing and community amenities — 2 code(s)
  - `EXP_FUNC_HOU_EXE` — Housing (COFOG 706)
  - **Water supply**
    - `EXP_FUNC_WAT_SAN_EXE` — Spending in water and sanitation

### Public order and safety — 1 code(s)
  - `EXP_FUNC_PUB_SAF_EXE` — Spending in public safety

### Recreation, culture and religion — 1 code(s)
  - `EXP_FUNC_REV_CUS_EXC_EXE` — Recreation, culture and religion (COFOG 708) _⚠ code token REV_CUS_EXC appears mislabeled: the workbook's category is 'Recreation, culture and religion (COFOG 708)' — rename the code upstream or treat as COFOG 708_

### Social protection — 1 code(s)
  - `EXP_FUNC_SOC_PRO_EXE` — Social Protection (COFOG 710)


## Revenue — Economic classification (REV_ECON)

### Customs and excise — 8 code(s)
  - `REV_ECON_CUS_EXC_EXE` — Revenues: Customs/excise
  - `REV_ECON_CUS_EXE` — Revenues: Customs
  - `REV_ECON_EXC_EXE` — Revenues: Excises
  - **Customs (exports)**
    - `REV_ECON_CUS_EXP_EXE` — Revenue: Customs (exports)
  - **Customs (imports)**
    - `REV_ECON_CUS_IMP_EXE` — Revenue: Customs (imports)
  - **Excises (alcohol)**
    - `REV_ECON_EXC_ALC_EXE` — Revenues: Excises (Alcohol)
  - **Excises (fuels)**
    - `REV_ECON_EXC_FUE_EXE` — Revenues: Excises (fuels)
  - **Excises (tobacco)**
    - `REV_ECON_EXC_TOB_EXE` — Revenues: Excises (tobacco)

### Income tax — 3 code(s)
  - `REV_ECON_INC_TAX_EXE` — Revenues: Income tax
  - **Corporate income tax**
    - `REV_ECON_COR_INC_TAX_EXE` — Revenues: Corporate Income tax
  - **Personal income tax**
    - `REV_ECON_PER_INC_TAX_EXE` — Revenues: Personal Income tax

### Other revenue — 1 code(s)
  - **Permits/Fees**
    - `REV_ECON_PER_FEE_EXE` — Revenues: Permits/Fees

### Property taxes — 2 code(s)
  - `REV_ECON_PRO_TAX_EXE` — Revenue: Property taxes
  - **Immovable**
    - `REV_ECON_PRO_TAX_IMM_EXE` — Revenue: Property taxes (immovable)

### Social contributions — 1 code(s)
  - `REV_ECON_SOC_CON_EXE` — Revenue: Social contributions

### Taxes on goods and services — 1 code(s)
  - **VAT**
    - `REV_ECON_VAT_EXE` — Revenues: VAT

### (no assignment yet)
  - `REV_ECON_SBN_OWN_EXE` — Revenues: Subnational own revenues


## Review questions for the SME

1. Any code parked under the wrong `econ` / `func` parent?
2. For codes carrying `⚠` notes (subnational `SBN_*`, mislabeled `REV_CUS_EXC`, any unmatched tokens): decide canonical treatment.
3. `EXP_CROSS_*` codes are excluded to avoid double-counting against the `EXP_ECON` × `EXP_FUNC` parents. If the SME wants any specific CROSS kept as a primary dimension, call it out.
