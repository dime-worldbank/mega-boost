# Uganda overcounting detection report

- Microdata rows (validatable years 2005/06..2023/24): **430,840**
- Validation: **3088/3090** (code, year) cells reproduce Excel within 0.5%
- Parser correct on **3090/3090** cells (0 unexplained mismatch(es), 2 confirmed Excel formula error(s) where the python value is the correct one)

### Confirmed Excel formula errors (python value is correct)

| code | year | excel (wrong) | python (correct) | rel |
|---|---|--:|--:|--:|
| EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE | 2022/23 | 186,260,624,000 | 93,130,312,000 | 50.0% |
| EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE | 2023/24 | 208,808,794,896 | 104,404,397,448 | 50.0% |

See verification.md for the corrected SUMIFS of each. Notes:
- **EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE** (2022/23, 2023/24): Array-broadcast double-count. Formula is SUM(SUMIFS(...,admin1,{"districts","Urban/Municipals"},...,econ2,"31*") + SUM(SUMIFS(...,econ2,"23 CONSUMPTION OF FIXED ASSETS"))): the inner SUM(...) collapses the 2nd term to a SCALAR which Excel then broadcasts across the 2-element {districts,Urban/Municipals} array of the 1st term, adding the '23 consumption' total twice. Excel cached is ~2x; the python value is correct. Corrected formula wraps BOTH SUMIFS in their own SUM() before adding -- see verification.md. (Same fragile shape: EXP_CROSS_SBN_CAP_EXP_ENE_EXE, EXP_CROSS_SBN_CAP_EDU_EXE, EXP_CROSS_SBN_REC_EXP_ENE_EXE -- latent only because their doubled term is ~0 in the validated years.)

## Overcounting within `econ` (flat, no hierarchy assumed)

| code_a | code_b | lines | Σ executed overlap | years |
|---|---|--:|--:|---|
| Wage bill | Capital expenditures | 1,672 | 6,949,196,980,400 | 2022/23,2023/24 |
| Capital expenditures | Goods and services | 3,281 | 2,562,845,609,160 | 2022/23,2023/24 |
| Capital expenditures | Other grants/transfers | 2,107 | 970,078,861,866 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2012/13,2013/14,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20 |
| Other grants/transfers | Social benefits | 8 | 523,991,667,608 | 2020/21,2021/22,2022/23,2023/24 |
| Wage bill | Other grants/transfers | 1,699 | 263,008,330,277 | 2005/06,2006/07,2007/08,2008/09,2009/10,2011/12,2012/13,2013/14,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22 |
| Goods and services | Other grants/transfers | 1,297 | 179,312,872,074 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2012/13,2013/14,2014/15,2015/16 |
| Goods and services | Social benefits | 241 | 12,323,206,212 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2012/13,2013/14,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |
| Wage bill | Social benefits | 60 | 7,199,664,746 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2012/13,2013/14,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |

## Overcounting within `econ_sub` (flat, no hierarchy assumed)

| code_a | code_b | lines | Σ executed overlap | years |
|---|---|--:|--:|---|
| Allowances | Social Assistance | 14 | 84,787,602 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2022/23 |
| Recurrent maintenance | Social Assistance | 13 | 42,406,491 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2013/14,2020/21,2022/23 |

## Overcounting within `func` (flat, no hierarchy assumed)

| code_a | code_b | lines | Σ executed overlap | years |
|---|---|--:|--:|---|
| Education | Social protection | 63 | 200,581,782,748 | 2008/09,2009/10,2010/11,2011/12,2012/13,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |
| Public order and safety | Social protection | 109 | 192,694,420,949 | 2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |
| Health | Social protection | 416 | 183,215,940,371 | 2009/10,2011/12,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |
| Economic affairs | Social protection | 60 | 175,967,522,612 | 2009/10,2010/11,2011/12,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |
| Health | Education | 301 | 127,640,540,488 | 2022/23,2023/24 |
| Environmental protection | Housing and community amenities | 1,649 | 46,601,994,641 | 2005/06,2006/07,2007/08,2008/09,2009/10,2010/11,2011/12,2012/13,2013/14,2014/15,2015/16,2022/23,2023/24 |
| Social protection | Housing and community amenities | 38 | 19,513,544,436 | 2005/06,2013/14,2014/15,2015/16,2016/17,2017/18,2018/19,2019/20,2020/21,2021/22,2022/23,2023/24 |
| Health | Housing and community amenities | 665 | 11,475,779,063 | 2011/12,2012/13,2013/14 |
| Environmental protection | Social protection | 12 | 9,614,185,803 | 2005/06,2009/10,2013/14,2021/22,2022/23,2023/24 |
| Economic affairs | Education | 14 | 399,759,723 | 2022/23 |
| Defense | Social protection | 1 | 0 | 2022/23 |
| Economic affairs | Environmental protection | 1 | 0 | 2015/16 |
