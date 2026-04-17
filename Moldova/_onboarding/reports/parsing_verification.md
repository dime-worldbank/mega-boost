# Moldova formula parsing verification

Cross-checks every TAG rule in `data/tag_rules.csv` by applying its parsed `criteria_json` to the matching year-range raw sheet and comparing the per-year sum against Excel's cached cell value in the Approved / Executed sheet. A MATCH confirms that our parsed criteria reproduce Excel exactly; a MISMATCH points at a parser gap, a formula feature we don't yet model (IF-wrapping, cell arithmetic, INDIRECT), or a hand-entered override.


## Summary

| Status | Count |
|---|---:|
| `MATCH` | 4,148 |
| `MISMATCH` | 66 |
| `SKIP_UNSUPPORTED_MEASURE` | 2 |

Tolerance: |pipeline − excel| ≤ 0.5 currency units.

## Rules with at least one mismatch

One section per (code, sheet, range). Each shows the formula the extractor captured and the per-year diff for years where pipeline and excel disagree.

### `EXP_CROSS_CAP_EXP_WAT_SAN_EXE` — Executed, range=base (7 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed,year,C$1,transfer,"Excluding transfers",func2,"11.04 Water service",exp_type,"Capital")+SUM(SUMIFS('Raw2'!$F:$F,'Raw2'!$A:$A,C$1,'Raw2'!$E:$E,"Excluding transfers",'Raw2'!$B:$B,{"740 National water supply and sanitation project","904 Construction, Rehabilitation and Expansion of Water Supply and Sewerage Networks and Road Repairs in Chisinau Municipality Project","911 Water supply and sanitation program in Moldova(ApaSan)","923 Clean Water - for Beneficiaries of Localities","925 Improvement of Ecological State of Prut and Nistru Basins through Improvement of Waste Water Treatment Systems in Cernauti and Drochia","939 Water Supply System Rehabilitation Project in Nisporeni rayon","752 Water Supply Service Development Program"},'Raw2'!$D:$D,"Capital"))`

`n_sumifs = 2`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2009 | 150,480,000 | 0 | -150,480,000 | -100.00% |
| 2010 | 36,780,000 | 0 | -36,780,000 | -100.00% |
| 2011 | 90,290,000 | 6,730,000 | -83,560,000 | -92.55% |
| 2012 | 173,768,967 | 4,000,000 | -169,768,967 | -97.70% |
| 2013 | 139,947,171 | 107,400 | -139,839,771 | -99.92% |
| 2014 | 183,345,962 | 0 | -183,345,962 | -100.00% |
| 2015 | 102,309,800 | 0 | -102,309,800 | -100.00% |

### `EXP_FUNC_WAT_SAN_EXE` — Executed, range=base (7 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed,year,C$1,transfer,"Excluding transfers",func2,"11.04 Water service")+SUM(SUMIFS('Raw2'!$F:$F,'Raw2'!$A:$A,C$1,'Raw2'!$E:$E,"Excluding transfers",'Raw2'!$B:$B,{"740 National water supply and sanitation project","904 Construction, Rehabilitation and Expansion of Water Supply and Sewerage Networks and Road Repairs in Chisinau Municipality Project","911 Water supply and sanitation program in Moldova(ApaSan)","923 Clean Water - for Beneficiaries of Localities","925 Improvement of Ecological State of Prut and Nistru Basins through Improvement of Waste Water Treatment Systems in Cernauti and Drochia","939 Water Supply System Rehabilitation Project in Nisporeni rayon","752 Water Supply Service Development Program"}))`

`n_sumifs = 2`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2009 | 169,943,400 | 19,463,400 | -150,480,000 | -88.55% |
| 2010 | 52,752,500 | 15,972,500 | -36,780,000 | -69.72% |
| 2011 | 103,262,500 | 19,702,500 | -83,560,000 | -80.92% |
| 2012 | 186,869,467 | 17,100,500 | -169,768,967 | -90.85% |
| 2013 | 153,040,751 | 13,200,980 | -139,839,771 | -91.37% |
| 2014 | 198,818,462 | 15,472,500 | -183,345,962 | -92.22% |
| 2015 | 118,732,300 | 16,422,500 | -102,309,800 | -86.17% |

### `EXP_CROSS_WAG_PUB_SAF_EXE` — Approved, range=20 (5 mismatched year(s))

Captured formula (sample year): `=SUMIFS(approved_20,year_20,Q$1,transfer_20,"Cu exceptia transferurilor", econ0_20,"Expenditures",func1_20,"0300 Ordine publica si securitate nationala",econ2_20,"210000 Cheltuieli de personal")-Q33`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2020 | 461,958,900 | 1,217,609,600 | 755,650,700 | 163.58% |
| 2021 | 514,546,600 | 1,284,638,400 | 770,091,800 | 149.66% |
| 2022 | 541,939,900 | 1,371,710,100 | 829,770,200 | 153.11% |
| 2023 | 644,878,200 | 1,595,726,100 | 950,847,900 | 147.45% |
| 2024 | 664,104,300 | 1,764,259,600 | 1,100,155,300 | 165.66% |

### `EXP_CROSS_WAG_PUB_SAF_EXE` — Executed, range=20 (5 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed_20,year_20,Q$1,transfer_20,"Cu exceptia transferurilor", econ0_20,"Expenditures",func1_20,"0300 Ordine publica si securitate nationala",econ2_20,"210000 Cheltuieli de personal")-Q33`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2020 | 452,934,594 | 1,195,660,340 | 742,725,745 | 163.98% |
| 2021 | 527,500,184 | 1,302,535,879 | 775,035,695 | 146.93% |
| 2022 | 565,806,720 | 1,393,490,120 | 827,683,400 | 146.28% |
| 2023 | 653,774,390 | 1,612,779,974 | 959,005,584 | 146.69% |
| 2024 | 751,978,865 | 1,847,890,540 | 1,095,911,676 | 145.74% |

### `EXP_FUNC_PUB_SAF_EXE` — Approved, range=20 (5 mismatched year(s))

Captured formula (sample year): `=SUMIFS(approved_20,year_20,Q$1,transfer_20,"Cu exceptia transferurilor", econ0_20,"Expenditures",func1_20,"0300 Ordine publica si securitate nationala")-Q32`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2020 | 4,230,959,500 | 5,272,749,200 | 1,041,789,700 | 24.62% |
| 2021 | 4,306,775,400 | 5,347,373,600 | 1,040,598,200 | 24.16% |
| 2022 | 4,727,519,700 | 5,849,771,600 | 1,122,251,900 | 23.74% |
| 2023 | 5,251,173,300 | 6,498,347,300 | 1,247,174,000 | 23.75% |
| 2024 | 5,160,975,400 | 6,565,647,600 | 1,404,672,200 | 27.22% |

### `EXP_FUNC_PUB_SAF_EXE` — Executed, range=20 (5 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed_20,year_20,Q$1,transfer_20,"Cu exceptia transferurilor", econ0_20,"Expenditures",func1_20,"0300 Ordine publica si securitate nationala")-Q32`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2020 | 3,825,956,154 | 4,770,417,196 | 944,461,042 | 24.69% |
| 2021 | 4,107,893,498 | 5,112,365,468 | 1,004,471,970 | 24.45% |
| 2022 | 4,846,705,863 | 5,956,422,316 | 1,109,716,453 | 22.90% |
| 2023 | 5,464,986,645 | 6,712,552,283 | 1,247,565,638 | 22.83% |
| 2024 | 5,735,525,871 | 7,137,265,879 | 1,401,740,008 | 24.44% |

### `EXP_FUNC_SOC_PRO_EXE` — Approved, range=20 (5 mismatched year(s))

Captured formula (sample year): `=SUMIFS(approved_20,year_20,Q$1,transfer_20,"Cu exceptia transferurilor", econ0_20,"Expenditures",func1_20,"1000 Protectie sociala")+Q17`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2020 | 26,755,741,260 | 2,123,141,260 | -24,632,600,000 | -92.06% |
| 2021 | 30,079,987,900 | 2,214,087,900 | -27,865,900,000 | -92.64% |
| 2022 | 31,743,857,300 | 3,877,957,300 | -27,865,900,000 | -87.78% |
| 2023 | 46,453,688,500 | 8,453,688,500 | -38,000,000,000 | -81.80% |
| 2024 | 46,894,460,300 | 4,194,460,300 | -42,700,000,000 | -91.06% |

### `EXP_FUNC_SOC_PRO_EXE` — Executed, range=20 (5 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed_20,year_20,Q$1,transfer_20,"Cu exceptia transferurilor", econ0_20,"Expenditures",func1_20,"1000 Protectie sociala")+Q17`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2020 | 26,495,408,274 | 1,862,808,274 | -24,632,600,000 | -92.97% |
| 2021 | 30,212,528,251 | 2,346,628,251 | -27,865,900,000 | -92.23% |
| 2022 | 33,046,973,346 | 5,181,073,346 | -27,865,900,000 | -84.32% |
| 2023 | 45,206,638,003 | 7,206,638,003 | -38,000,000,000 | -84.06% |
| 2024 | 47,349,053,174 | 4,649,053,174 | -42,700,000,000 | -90.18% |

### `EXP_CROSS_WAG_PUB_SAF_EXE` — Approved, range=16 (4 mismatched year(s))

Captured formula (sample year): `=SUMIFS(approved_16,year_16,M$1,transfer_16,"Cu exceptia transferurilor", econ0_16,"Expenditures",func1_16,"0300 Ordine publica si securitate nationala",econ2_16,"210000 Cheltuieli de personal")-M33`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2016 | 280,170,200 | 779,059,608 | 498,889,408 | 178.07% |
| 2017 | 366,608,700 | 964,665,400 | 598,056,700 | 163.13% |
| 2018 | 390,019,400 | 1,053,892,550 | 663,873,150 | 170.22% |
| 2019 | 407,221,790 | 1,118,575,600 | 711,353,810 | 174.68% |

### `EXP_CROSS_WAG_PUB_SAF_EXE` — Executed, range=16 (4 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed_16,year_16,M$1,transfer_16,"Cu exceptia transferurilor", econ0_16,"Expenditures",func1_16,"0300 Ordine publica si securitate nationala",econ2_16,"210000 Cheltuieli de personal")-M33`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2016 | 276,565,297 | 722,302,591 | 445,737,294 | 161.17% |
| 2017 | 364,151,838 | 936,032,952 | 571,881,114 | 157.04% |
| 2018 | 403,330,205 | 1,017,444,979 | 614,114,774 | 152.26% |
| 2019 | 387,670,500 | 1,089,068,634 | 701,398,134 | 180.93% |

### `EXP_FUNC_PUB_SAF_EXE` — Approved, range=16 (4 mismatched year(s))

Captured formula (sample year): `=SUMIFS(approved_16,year_16,M$1,transfer_16,"Cu exceptia transferurilor", econ0_16,"Expenditures",func1_16,"0300 Ordine publica si securitate nationala")-M32`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2016 | 2,814,893,836 | 3,584,040,844 | 769,147,008 | 27.32% |
| 2017 | 3,195,856,296 | 4,049,388,096 | 853,531,800 | 26.71% |
| 2018 | 3,553,564,760 | 4,459,092,110 | 905,527,350 | 25.48% |
| 2019 | 3,771,539,600 | 4,755,278,400 | 983,738,800 | 26.08% |

### `EXP_FUNC_PUB_SAF_EXE` — Executed, range=16 (4 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed_16,year_16,M$1,transfer_16,"Cu exceptia transferurilor", econ0_16,"Expenditures",func1_16,"0300 Ordine publica si securitate nationala")-M32`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2016 | 2,693,281,801 | 3,337,462,621 | 644,180,820 | 23.92% |
| 2017 | 3,218,854,595 | 4,040,451,157 | 821,596,563 | 25.52% |
| 2018 | 3,567,144,824 | 4,376,236,642 | 809,091,818 | 22.68% |
| 2019 | 3,438,769,209 | 4,322,198,615 | 883,429,406 | 25.69% |

### `EXP_ECON_SOC_ASS_EXE` — Approved, range=base (3 mismatched year(s))

Captured formula (sample year): `=SUMIFS(approved,year,C$1,func1,"10 Social care and social insurance",transfer,"Excluding transfers")-C19`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2006 | 471,763,399 | 5,066,920,599 | 4,595,157,200 | 974.04% |
| 2007 | 584,074,087 | 6,194,521,587 | 5,610,447,500 | 960.57% |
| 2008 | 940,240,200 | 7,801,309,584 | 6,861,069,384 | 729.71% |

### `EXP_ECON_SOC_ASS_EXE` — Executed, range=base (3 mismatched year(s))

Captured formula (sample year): `=SUMIFS(executed,year,C$1,func1,"10 Social care and social insurance",transfer,"Excluding transfers")-C19`

`n_sumifs = 1`

| year | excel | pipeline | diff | rel |
|---:|---:|---:|---:|---:|
| 2006 | 501,163,944 | 5,174,825,555 | 4,673,661,611 | 932.56% |
| 2007 | 909,907,440 | 6,621,048,904 | 5,711,141,463 | 627.66% |
| 2008 | 1,061,293,388 | 7,915,492,326 | 6,854,198,939 | 645.83% |

## Known unmodeled formula features

Two categories of mismatch are expected and documented here so the SME can decide whether to accept or resolve upstream:

- **Cell-subtraction** — `=SUMIFS(…) - C19`. The SUMIFS result is adjusted by subtracting another cell's value (usually a sibling tag-row's override). The parser captures the SUMIFS but discards the trailing `-cell_ref`, so pipeline > excel by that cell's value. Affects `SOC_ASS`, `PUB_SAF`, `SOC_PRO`.

- **Raw2 supplements** — `=SUMIFS(…) + SUM(SUMIFS('Raw2'!$F:$F, …))`. The second SUMIFS pulls from the hidden `Raw2` sheet, which uses a different schema (`admin6` instead of `admin1`, no `econ0`). Our silver mapping doesn't include Raw2, so that branch's contribution is dropped. Affects `WAT_SAN`.
