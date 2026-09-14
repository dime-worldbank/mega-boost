# Databricks notebook source
# Bulgaria BOOST transform + load (DLT).
#
# Emulates the workbook's `Executed` / `Approved` sheet classification as a PER-LINE pipeline: every
# microdata row gets exactly one econ / econ_sub / func / func_sub. See verification.md for the issue
# log, the corrected workbook formulas and the expert sign-off.
#
# Principles (same as Uganda):
#   1. ORDER-PROOF, NO PRIORITY. Each category is a mutually-exclusive predicate, disjoint BY
#      CONSTRUCTION: every overlap the detector found is resolved by an explicit discriminating
#      criterion baked into the predicate. The `.when()` chains are a readability device only --
#      reordering the branches must not change any row's tag. `bgr_boost_silver` asserts this
#      (exactly one econ and one func per row; at most one econ_sub / func_sub).
#   2. YEAR-AWARE BY CONSTRUCTION. The workbook's 2006-2008 cells use wildcard criteria ("05*",
#      "2.3*", "43*") because those years carry labels truncated to 20 characters; the 2009+ cells use
#      the full labels. Matching on the CODE PREFIX ("05 ", "2.3 ", "43 ") reproduces both variants
#      exactly (BGR_detect_overcounting.py reproduces 3,126 / 3,126 workbook cells), so no year branch
#      is needed. Labels are matched literally otherwise (no trimming), as Excel's SUMIFS does.
#
# Source columns (Expenditure.csv, one row per budget line; the workbook's named ranges in brackets):
#   year, admin1 ("1 Central"/"2 Local"/"3 Other"), func1..func3 (COFOG-like Bulgarian functional
#   classification), econ1/econ2 (paragraph/sub-paragraph), fin_source1 [source], exp_type
#   ("1 Personnel"/"2 Non-personnel recurrent"/"3 Capital"), transfer, adjusted [approved], executed,
#   roads [road] and Interest [interest] -- the two per-line helper flags ("road from func3",
#   "interest from econ1").
import dlt
from pyspark.sql.functions import col, lower, trim, when, lit, coalesce, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Bulgaria'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {"header": "true", "multiline": "true", "quote": '"', "escape": '"'}

# ---------------------------------------------------------------------------------------------
# Decisions (verification.md, "Decisions"). Flip a knob here and the exclusions move with it.
# ---------------------------------------------------------------------------------------------
# Q1 -- when a helper flag disagrees with the code it is documented to derive from, which owns the
#       line? `Interest` is "interest from econ1" (paragraphs 21-29) but 30 lines in 2023 carry the flag
#       on wage / goods / subsidy / capital paragraphs (12.3M); `roads` is "road from func3"
#       (831-834, 849) but 388 lines in 2023 carry it on Recreation & culture activities (274.9M).
#       False = the CODE owns the line (default); True = the FLAG owns it (the workbook's literal reading).
HELPER_FLAG_WINS = False
# Q2 -- 97 lines in 2020 (4.03B executed) are shifted one column: econ1 holds the exp_type label
#       ("2 Non-personnel recurrent") and econ2 the econ1 label ("10.00 Maintenance"). Every econ1-based
#       category misses them (Goods and services -542M, pension contributions -1.02B, ...).
#       False = as in the workbook (default); True = read the econ1 code back from econ2 for those lines.
REPAIR_2020_SHIFTED_ROWS = False
# Q3 -- func_sub "Water Supply" = EXP_FUNC_WAT_SAN_EXE = func3 603 Sewerage (under 6.1 Housing) + 626
#       Purification of wastewater (under 6.2 Environment, where the workbook also counts it as Waste
#       water management). True = both (default, as in the workbook); False = 603 only.
WATSAN_INCLUDES_626 = True

# Foreign-funded sources (EXP_ECON_TOT_EXP_FOR_EXE), matched on the full label.
FOREIGN_SOURCES = ["5 other international programmes", "7 other european funds",
                   "8 eu agricultural fund", "9 eu cohesion and structural funds"]


# COMMAND ----------

@dlt.table(name='bgr_boost_bronze')
def boost_bronze():
    return (spark.read.format("csv").options(**CSV_READ_OPTIONS).option("inferSchema", "true")
            .load(f'{COUNTRY_MICRODATA_DIR}/Expenditure.csv'))


# COMMAND ----------

# NULL-safety: in Spark `col == x` / `col.startswith(x)` return NULL (not False) on a NULL column, and
# `NOT NULL` is still NULL -- an unguarded `~flag` would poison every `... & ~flag` predicate. Every
# atomic boolean below is therefore wrapped to return False on NULL.
def nz(c):
    return coalesce(c, lit(False))


def sw(colname, prefix):
    """Null-safe, case-insensitive startswith on the raw label (no trimming, as in Excel)."""
    return nz(lower(col(colname).cast('string')).startswith(prefix.lower()))


def any_sw(colname, prefixes):
    out = None
    for p in prefixes:
        c = sw(colname, p)
        out = c if out is None else (out | c)
    return out


def is_y(colname):
    """Helper flag truthiness: 'y' when set, blank/null otherwise."""
    return nz(lower(trim(col(colname).cast('string'))) == 'y')


@dlt.table(name='bgr_boost_silver')
@dlt.expect("exactly_one_econ", "n_econ = 1")
@dlt.expect("exactly_one_func", "n_func = 1")
@dlt.expect("at_most_one_econ_sub", "n_econ_sub <= 1")
@dlt.expect("at_most_one_func_sub", "n_func_sub <= 1")
def boost_silver():
    df = (dlt.read('bgr_boost_bronze')
          .withColumn('year', col('year').cast(IntegerType()))
          .filter(col('year').isNotNull()))

    if REPAIR_2020_SHIFTED_ROWS:  # Q2: "10.00 Maintenance" (econ2) -> "10 Maintenance" (econ1)
        shifted = nz(lower(trim(col('econ1').cast('string')))
                     .isin('1 personnel', '2 non-personnel recurrent', '3 capital'))
        df = df.withColumn('econ1', when(shifted, regexp_replace(col('econ2').cast('string'), r'^(\d\d)\.00 ', '$1 '))
                                    .otherwise(col('econ1')))

    # ---- admin / geo ----
    # admin1 "2 Local" = municipalities (EXP_ECON_SBN_TOT_SPE_EXE); "1 Central" and "3 Other" (the social
    # security funds) are central government. The workbook's Expenditure sheet carries no region or
    # ministry column, so admin1/geo1 (region) and admin2 (ministry) cannot be filled -- see verification.md.
    is_local = sw('admin1', '2 ')
    df = (df
          .withColumn('admin0', when(is_local, lit('Regional')).otherwise(lit('Central')))
          .withColumn('admin1', when(is_local, lit(None).cast('string')).otherwise(lit('Central Scope')))
          .withColumn('admin2', lit(None).cast('string'))
          .withColumn('geo0', col('admin0'))
          .withColumn('geo1', col('admin1'))
          .withColumn('is_foreign', nz(lower(trim(col('fin_source1').cast('string'))).isin(FOREIGN_SOURCES))))

    # ================= econ (8 disjoint categories) =================
    # The workbook splits by exp_type for Wage bill / Capital expenditures and by econ1 paragraph for the
    # rest; econ1 paragraphs 01/02/05 are always "1 Personnel" and 51-57 always "3 Capital", so the two
    # families never meet in the data -- the `~et1 & ~et3` guards below only make that disjointness hold
    # by construction. Interest (Q1) is the one genuine overlap and is excluded from every other category.
    et1 = sw('exp_type', '1 ')                      # "1 Personnel"
    et3 = sw('exp_type', '3 ')                      # "3 Capital"
    e10 = sw('econ1', '10 ')                        # "10 Maintenance"
    e_sub = any_sw('econ1', ['43 ', '44 ', '45 '])  # subsidies to non-financial / financial / non-profit
    e39, e41, e42 = sw('econ1', '39 '), sw('econ1', '41 '), sw('econ1', '42 ')
    e05 = sw('econ1', '05 ')                        # employer social security contributions
    f1_5 = sw('func1', '5 ')                        # "5 Social protection"
    intr_code = any_sw('econ1', ['21 ', '22 ', '25 ', '26 ', '27 ', '28 ', '29 '])
    intr_flag = is_y('Interest')
    intr = intr_flag if HELPER_FLAG_WINS else intr_code

    wage = et1 & ~intr
    capex = et3 & ~intr
    goods = e10 & ~et1 & ~et3 & ~intr
    subs = e_sub & ~et1 & ~et3 & ~intr
    pens = e41 & ~et1 & ~et3 & ~intr
    socass = e42 & f1_5 & ~et1 & ~et3 & ~intr
    socben = pens | socass                          # EXP_ECON_SOC_BEN_EXE = SUM(Social assistance, Pensions)
    grant = (e39 | e42) & ~f1_5 & ~et1 & ~et3 & ~intr

    df = df.withColumn('econ',
        when(intr, 'Interest on debt')
        .when(wage, 'Wage bill')
        .when(capex, 'Capital expenditures')
        .when(goods, 'Goods and services')
        .when(subs, 'Subsidies')
        .when(socben, 'Social benefits')
        .when(grant, 'Other grants and transfers')
        .otherwise('Other expenses'))                # = Total - SUM(the seven), as in the workbook

    # econ_sub (names as in quality/transform_load_dlt.py; null where the workbook defines no sub)
    s_pencon = e05 & ~intr                          # EXP_ECON_PEN_CON_EXE (sits under Wage bill)
    s_capmai = sw('econ2', '51.00 ') & ~intr        # capital repair of fixed tangible assets
    s_basic = (sw('econ2', '10.11 ') | sw('econ2', '10.16 ')) & ~intr   # food; water, fuels and energy
    s_empcon = sw('econ2', '10.20 ') & ~intr        # expenses for external services
    s_recmai = sw('econ2', '10.30 ') & ~intr        # current repairs expenses
    df = df.withColumn('econ_sub',
        when(socass, 'Social Assistance')
        .when(pens, 'Pensions')
        .when(s_pencon, 'Social Benefits (pension contributions)')
        .when(s_capmai, 'Capital Maintenance')
        .when(s_basic, 'Basic Services')
        .when(s_empcon, 'Employment Contracts')
        .when(s_recmai, 'Recurrent Maintenance')
        .otherwise(lit(None).cast('string')))

    # ================= func (10 COFOG, disjoint) =================
    # Sectors are func1 groups; Defence / Public order split func1 "2 Defence and security" by func2, and
    # Environment / Housing split func1 "6 Housing, public works, utilities and environmental protection"
    # by func2. Gating the func2 tests on their func1 makes the ten predicates disjoint by construction
    # (the data is block-diagonal apart from two zero-value lines).
    def f1(p):
        return sw('func1', p)

    def f2(p):
        return sw('func2', p)

    is_def = f1('2 ') & f2('2.1 ')
    is_pos = f1('2 ') & any_sw('func2', ['2.2 ', '2.3 ', '2.4 ', '2.5 '])
    is_eco = f1('8 ')
    is_env = f1('6 ') & f2('6.2 ')
    is_hou = f1('6 ') & f2('6.1 ')
    is_hea = f1('4 ')
    is_rec = f1('7 ')
    is_edu = f1('3 ')
    is_socp = f1('5 ')

    df = df.withColumn('func',
        when(is_def, 'Defence')
        .when(is_pos, 'Public order and safety')
        .when(is_eco, 'Economic affairs')
        .when(is_env, 'Environmental protection')
        .when(is_hou, 'Housing and community amenities')
        .when(is_hea, 'Health')
        .when(is_rec, 'Recreation, culture and religion')
        .when(is_edu, 'Education')
        .when(is_socp, 'Social protection')
        .otherwise('General public services'))      # = Total - SUM(sectors): func1 1 and 9 n.e.c.

    # func_sub (most specific workbook leaf; names as in quality/transform_load_dlt.py; null otherwise).
    # Leaves are disjoint by their func3 / func2 codes; Transport (EXP_FUNC_TRA_EXE) is the sum of the
    # four transport leaves. The func / func_sub hierarchy is pending expert sign-off (verification.md).
    def f3(p):
        return sw('func3', p)

    roads_code = any_sw('func3', ['831 ', '832 ', '833 ', '834 ', '849 '])
    is_roads = is_y('roads') if HELPER_FLAG_WINS else roads_code
    is_jud = is_pos & f2('2.3 ')
    is_psaf = is_pos & ~f2('2.3 ')
    is_agr = f2('8.2 ')
    is_rail, is_watt, is_air = f3('835 '), f3('837 '), f3('836 ')
    is_ene = f2('8.1 ') & ~f3('808 ')
    is_tel = any_sw('func3', ['838 ', '839 ', '083 '])
    is_ws = f3('603 ') | (f3('626 ') if WATSAN_INCLUDES_626 else lit(False))
    is_pse = is_edu & f3('322 ')
    is_ter = is_edu & f3('341 ')
    df = df.withColumn('func_sub',
        when(is_jud, 'Judiciary')
        .when(is_psaf, 'Public Safety')
        .when(is_agr, 'Agriculture')
        .when(is_roads, 'Roads')
        .when(is_rail, 'Railroads')
        .when(is_watt, 'Water Transport')
        .when(is_air, 'Air Transport')
        .when(is_ene, 'Energy')
        .when(is_tel, 'Telecom')
        .when(is_ws, 'Water Supply')
        .when(is_pse, 'Primary and Secondary education')
        .when(is_ter, 'Tertiary Education')
        .otherwise(lit(None).cast('string')))

    # Exclusivity diagnostics for the @dlt.expect checks (the order-proof guarantee). They use the SAME
    # predicates as the .when() branches, so n = 1 everywhere proves no row is double-counted whatever
    # the branch order (n = 0 -> the residual category).
    econ_cats = [intr, wage, capex, goods, subs, socben, grant]
    n_econ = sum([c.cast(IntegerType()) for c in econ_cats])
    df = df.withColumn('n_econ', when(n_econ == 0, lit(1)).otherwise(n_econ))
    func_cats = [is_def, is_pos, is_eco, is_env, is_hou, is_hea, is_rec, is_edu, is_socp]
    n_func = sum([c.cast(IntegerType()) for c in func_cats])
    df = df.withColumn('n_func', when(n_func == 0, lit(1)).otherwise(n_func))
    df = df.withColumn('n_econ_sub', sum([c.cast(IntegerType()) for c in
                                          [socass, pens, s_pencon, s_capmai, s_basic, s_empcon, s_recmai]]))
    df = df.withColumn('n_func_sub', sum([c.cast(IntegerType()) for c in
                                          [is_jud, is_psaf, is_agr, is_roads, is_rail, is_watt, is_air,
                                           is_ene, is_tel, is_ws, is_pse, is_ter]]))
    return df


# COMMAND ----------

@dlt.table(name='bgr_boost_gold')
@dlt.expect_or_drop("executed_or_approved", "executed IS NOT NULL OR approved IS NOT NULL")
def boost_gold():
    # `adjusted` is the adjusted budget -- the column the workbook's `Approved` sheet sums (named range
    # `approved`). There is no separate revised budget, so revised = approved.
    return (dlt.read('bgr_boost_silver')
            .withColumn('country_name', lit(COUNTRY))
            .withColumn('approved', col('adjusted').cast(DoubleType()))
            .withColumn('revised', col('adjusted').cast(DoubleType()))
            .withColumn('executed', col('executed').cast(DoubleType()))
            .select('country_name', 'year', 'admin0', 'admin1', 'admin2', 'geo0', 'geo1',
                    'is_foreign', 'func', 'func_sub', 'econ', 'econ_sub',
                    'approved', 'revised', 'executed'))
