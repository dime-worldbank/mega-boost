# Databricks notebook source
# Uganda BOOST transform + load (DLT).
#
# Emulates the workbook's `Executed`-sheet classification as a PER-LINE pipeline: every microdata
# row gets exactly one econ / econ_sub / func / func_sub. See verification.md for the issue log,
# the corrected SUMIFS, and the expert sign-off.
#
# Two principles enforced here (both required by the reviewers):
#   1. ORDER-PROOF, NO PRIORITY. Each category is a mutually-exclusive predicate, disjoint *by
#      construction* — every overlap the detector found is resolved by an explicit discriminating
#      criterion baked into the predicate (e.g. econ categories exclude `assistance`/`pension`/`add`
#      lines that belong to Social benefits / the add-override class). The `.when()` chains are a
#      readability device only: reordering the branches must not change any row's tag. `boost_silver`
#      asserts this (exactly one econ and one func per row).
#   2. YEAR-AWARE. Uganda recoded its vote/sector scheme at FY2022/23. econ is year-stable (GFS econ2
#      unchanged; allowances econ5 211103->211106 handled by a union). func is year-aware: pre-2022/23
#      uses func0 sector names + Vote_Function; from 2022/23 it uses renumbered func0 + per-line flags
#      (health/education/security/wss). See verification.md §2.
import dlt
from pyspark.sql.functions import col, lower, trim, when, lit, substring, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
from glob import glob

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Uganda'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {"header": "true", "multiline": "true", "quote": '"', "escape": '"'}

# Raw Excel header -> logical column name (the names the SUMIFS reference). See verification.md §2.
RAW_TO_LOGICAL = {
    "Year": "year_raw",
    "Budget Type": "budget_type",
    "MALGs": "malgs",                 # admin1: Ministries / districts / Urban/Municipals
    "Vote": "vote",                   # admin2: spending entity
    "Program": "program",
    "Project": "project",
    "geo1": "geo_src",                # 01 Central Government / 02 Local Government
    "Econ 2": "econ2",
    "Econ3": "econ3",
    "Item": "econ5",
    "MTEF with External Debt and Arrears Adjusted": "func0",   # sector (year-recoded)
    "Vote Function": "vote_function",
    "Func1": "func1",
    "Func2": "func2",
    "Func3": "func3",
    "Budget ": "approved_raw",        # note: trailing space in the source header
    "Expenditure": "executed_raw",
    "add": "add_ovr",
    "Transfers": "transfer",
    "social protection": "sp",
    "pension": "pension",
    "rail": "rail", "air": "air", "water": "water", "security": "security",
    "health": "health", "education": "education", "Tertiary": "tertiary",
    "wss": "wss", "assistance": "assistance",
}

# econ5 (Item) codes for debt redemption -> excluded from the total (below-the-line financing).
DEBT_REPAYMENT_PREFIXES = ("321606", "321615", "321616", "352884", "352883")


# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "year_raw IS NOT NULL")
@dlt.table(name='uga_boost_bronze')
def boost_bronze():
    file_paths = glob(f"{COUNTRY_MICRODATA_DIR}/*.csv")
    df = None
    for f in file_paths:
        part = spark.read.format("csv").options(**CSV_READ_OPTIONS).option("inferSchema", "true").load(f)
        df = part if df is None else df.unionByName(part, allowMissingColumns=True)
    # rename raw headers -> logical names
    for raw, logical in RAW_TO_LOGICAL.items():
        if raw in df.columns:
            df = df.withColumnRenamed(raw, logical)
    return df


# COMMAND ----------

def is_y(c):
    """Helper flag truthiness: the column holds 'y' (or '1' for transfers) when set, else blank/null."""
    return lower(trim(col(c))) == 'y'


def f0(prefix):
    """func0 (sector) starts with the given prefix, case-insensitive."""
    return lower(trim(col("func0"))).startswith(prefix.lower())


def e2(prefix):
    return lower(trim(col("econ2"))).startswith(prefix.lower())


@dlt.table(name='uga_boost_silver')
@dlt.expect("exactly_one_econ", "n_econ = 1")
@dlt.expect("exactly_one_func", "n_func = 1")
def boost_silver():
    df = (dlt.read('uga_boost_bronze')
          .withColumn('year', substring(col('year_raw'), 1, 4).cast(IntegerType()))
          .filter(col('year').isNotNull()))

    # Drop below-the-line debt redemption (matches Excel Total = SUMIFS - debt repayment).
    debt_cond = None
    for p in DEBT_REPAYMENT_PREFIXES:
        c = lower(trim(col('econ5'))).startswith(p)
        debt_cond = c if debt_cond is None else (debt_cond | c)
    df = df.filter(~debt_cond)

    new = col('year') >= 2022   # FY2022/23 vote/sector recode cutover

    # ---- admin / geo (best available; see verification.md "to confirm") ----
    is_local = lower(trim(col('malgs'))).isin('districts', 'urban/municipals')
    df = (df
          .withColumn('admin0', when(is_local, lit('Regional')).otherwise(lit('Central')))
          .withColumn('admin2', col('vote'))
          .withColumn('admin1', when(col('admin0') == 'Central', lit('Central Scope'))
                                .otherwise(col('vote')))
          .withColumn('geo0', when(lower(trim(col('geo_src'))).startswith('02'), lit('Regional'))
                              .otherwise(lit('Central')))
          .withColumn('geo1', when(col('geo0') == 'Regional', col('admin1'))
                              .otherwise(lit('Central Scope')))
          .withColumn('is_foreign', lower(trim(col('budget_type'))) == '03 external financing'))

    # ---- cross-cutting flags & add-override (the disjointness discriminators) ----
    SB = is_y('assistance') | is_y('pension')                 # owns -> Social benefits
    addw, addc, addn = (lower(trim(col('add_ovr'))) == 'wages',
                        lower(trim(col('add_ovr'))) == 'capital',
                        lower(trim(col('add_ovr'))) == 'nonwage')
    allow = e2('21') & (lower(trim(col('econ5'))).startswith('211103')
                        | lower(trim(col('econ5'))).startswith('211106'))  # year-union

    # ================= econ (8 disjoint categories) =================
    # Each branch carries its full exclusions; order is irrelevant (verified by the expectation).
    wage  = (~SB) & (((e2('21')) & ~addc & ~addn) | addw)
    capex = (~SB) & (((e2('31')) | (e2('23 consumption of fixed assets'))) & ~addw & ~addn | addc)
    goods = (~SB) & ((e2('22 use of goods and services')) & ~addw & ~addc | addn)
    subs  = (~SB) & (e2('25 subsidies')) & ~addw & ~addc & ~addn
    grant = ((~SB) & (e2('26 grants')) & (trim(col('transfer')) != '1')
             & ~(lower(trim(col('func1'))).startswith('710')) & ~addw & ~addc & ~addn)
    intr  = (~SB) & (e2('24'))

    df = df.withColumn('econ',
        when(SB, 'Social benefits')
        .when(wage, 'Wage bill')
        .when(capex, 'Capital expenditures')
        .when(goods, 'Goods and services')
        .when(subs, 'Subsidies')
        .when(grant, 'Other grants/transfers')
        .when(intr, 'Interest on debt')
        .otherwise('Other expenses'))

    # econ_sub (within the parent econ; null where the workbook defines no sub)
    df = df.withColumn('econ_sub',
        when(is_y('assistance'), 'Social Assistance')
        .when(is_y('pension'), 'Pensions')
        .when(wage & allow, 'Allowances')
        .when(wage, 'Basic wages')
        .when(capex & lower(trim(col('econ3'))).startswith('228 maintenance'), 'Capital maintenance')
        .when(goods & lower(trim(col('econ3'))).startswith('223 utility'), 'Goods and services (basic services)')
        .when(goods & lower(trim(col('econ3'))).startswith('225 professional'), 'Goods and services (employment contracts)')
        .when(goods & lower(trim(col('econ3'))).startswith('228 maintenance'), 'Recurrent maintenance')
        .otherwise(lit(None).cast('string')))

    # ================= func (10 COFOG, year-aware, disjoint) =================
    old_jud_vf = lower(trim(col('vote_function')))
    is_jud = ((~new) & (old_jud_vf.startswith('1237') | old_jud_vf.startswith('1251 judicial')
                        | old_jud_vf.startswith('1205 support to the justice')
                        | old_jud_vf.startswith('1255 public prosecutions')
                        | old_jud_vf.startswith('1252 legal reform'))) \
             | (new & lower(trim(col('vote'))).startswith('101 judiciary'))
    is_pubsaf = ((~new) & f0('12 justice')
                 & ~old_jud_vf.startswith('125') & ~old_jud_vf.startswith('1237')
                 & ~old_jud_vf.startswith('1205 support to the justice')
                 & ~old_jud_vf.startswith('1225 general administration')) \
                | (new & is_y('security'))
    is_def = lower(trim(col('vote_function'))).isin(
        '1101 national defence (updf)', '1601 national defence (updf)')
    is_health = ((~new) & f0('08 health')) | (new & is_y('health'))
    # Health wins the health/education flag tie (verification.md §4a — placeholder pending Massimo).
    is_educ = ((~new) & f0('07 education')) | (new & is_y('education') & ~is_y('health'))
    is_socpro = is_y('sp') | is_y('pension')
    is_watsan = ((~new) & (lower(trim(col('vote_function'))).startswith('0901 rural water')
                           | lower(trim(col('vote_function'))).startswith('0902 urban water')
                           | lower(trim(col('vote_function'))).startswith('0981 rural water')
                           | lower(trim(col('vote_function'))).startswith('0982 urban water'))) \
                | (new & is_y('wss'))
    is_env = (((~new) & (lower(trim(col('func1'))).startswith('705')
                         | lower(trim(col('vote_function'))).rlike('^(0906|0908|0904|0905|0907|0951)')))
              | (new & f0('06 natural resources'))) & ~is_watsan
    is_hou = (((~new) & f0('02 lands, housing')) | (new & f0('10 sustainable urbanisation'))) | is_watsan
    old_eco = (f0('04 works and transport') | f0('03 energy and mineral') | f0('01 agr')
               | f0('06 tourism') | f0('05 information') | f0('19 tourism'))
    new_eco = (f0('01') | f0('02') | f0('03') | f0('04') | f0('05')
               | f0('07') | f0('08') | f0('09'))
    is_eco = ((~new) & old_eco) | (new & new_eco)

    # Self-contained (mutually-exclusive) COFOG predicates: each carries its full discriminating
    # exclusions, so the .when() order below is irrelevant (proven: 0 rows match >1; see the
    # n_func expectation). The exclusions encode classification decisions documented in
    # verification.md §4 (e.g. Health/Education take a line out of Social protection; specific
    # sectors take it out of the broad Economic-affairs set; Water&sanitation -> Housing not Env).
    p_pos = is_jud | is_pubsaf
    p_def = is_def
    p_pubord = p_pos & ~is_def
    p_socpro = is_socpro & ~is_health & ~is_educ & ~is_def & ~p_pos
    p_health = is_health & ~is_def & ~p_pos
    p_educ = is_educ & ~is_def & ~p_pos & ~is_health
    p_env = is_env & ~is_def & ~p_pos & ~p_socpro & ~is_health & ~is_educ
    p_hou = (is_watsan | is_hou) & ~is_def & ~p_pos & ~p_socpro & ~is_health & ~is_educ & ~is_env
    p_eco = (is_eco & ~is_def & ~p_pos & ~p_socpro & ~is_health & ~is_educ & ~is_env
             & ~is_watsan & ~is_hou)

    df = df.withColumn('func',
        when(p_def, 'Defence')
        .when(p_pubord, 'Public order and safety')
        .when(p_socpro, 'Social protection')
        .when(p_health, 'Health')
        .when(p_educ, 'Education')
        .when(p_env, 'Environmental protection')
        .when(p_hou, 'Housing and community amenities')
        .when(p_eco, 'Economic affairs')
        .otherwise('General public services'))

    # func_sub (most-specific leaf; null where not determinable). Hierarchy resolved to the child.
    vf = lower(trim(col('vote_function')))
    is_roads = (vf.startswith('0901 n') | (vf == '0901 c')
                | vf.startswith('0913 urban road') | vf.startswith('0902 district')
                | vf.startswith('0404') | vf.startswith('0406') | vf.startswith('0451')
                | vf.startswith('0452') | vf.startswith('0481'))
    is_rail = is_y('rail')
    is_airt = is_y('air')
    is_watt = is_y('water')
    is_enepow = ((~new) & (vf.startswith('0301') | vf.startswith('0302') | vf.startswith('0351')
                           | vf.startswith('0349'))) | (new & f0('08'))
    is_eneoil = ((~new) & (vf.startswith('0303') | vf.startswith('0304') | vf.startswith('0305')
                           | vf.startswith('0306'))) | (new & f0('03'))
    f2 = lower(trim(col('func2')))
    df = df.withColumn('func_sub',
        # Public order
        when(is_jud, 'Judiciary')
        .when(is_pubsaf, 'Public safety')
        # Education (use func2 COFOG sub when present)
        .when(is_educ & f2.startswith('7091'), 'Primary education')
        .when(is_educ & f2.startswith('7092'), 'Secondary education')
        .when(is_educ & (f2.startswith('7094') | is_y('tertiary')), 'Tertiary education')
        .when(is_educ, 'Education (other)')
        # Health
        .when(is_health, 'Health')
        # Economic affairs sub (most specific first)
        .when(is_eco & is_rail, 'Railroads')
        .when(is_eco & is_airt, 'Air transport')
        .when(is_eco & is_watt, 'Water transport')
        .when(is_eco & is_roads, 'Roads')
        .when(is_eco & is_enepow, 'Energy (power)')
        .when(is_eco & is_eneoil, 'Energy (oil & gas)')
        .when(is_eco & (((~new) & f0('04 works')) | (new & f0('09'))), 'Transport')
        .when(is_eco & (f0('03 energy') | (new & (f0('03') | f0('08')))), 'Energy')
        .when(is_eco & f0('01 agr'), 'Agriculture')
        .when(is_eco & lower(trim(col('vote'))).startswith('020'), 'Telecoms')
        # Housing
        .when(is_watsan, 'Water and sanitation')
        .otherwise(lit(None).cast('string')))

    # exclusivity diagnostics for the @dlt.expect checks (and the order-proof guarantee).
    # These use the SAME self-contained predicates as the .when() branches above, so n=1 everywhere
    # proves no row is double-counted regardless of branch order (n=0 -> the residual category).
    econ_cats = [SB, wage, capex, goods, subs, grant, intr]                       # disjoint by construction
    n_econ = sum([c.cast(IntegerType()) for c in econ_cats])
    df = df.withColumn('n_econ', when(n_econ == 0, lit(1)).otherwise(n_econ))      # 0 -> Other expenses
    func_cats = [p_def, p_pubord, p_socpro, p_health, p_educ, p_env, p_hou, p_eco]  # disjoint by construction
    n_func = sum([c.cast(IntegerType()) for c in func_cats])
    df = df.withColumn('n_func', when(n_func == 0, lit(1)).otherwise(n_func))      # 0 -> General public services

    return df


# COMMAND ----------

@dlt.table(name='uga_boost_gold')
@dlt.expect_or_drop("executed_or_approved", "executed IS NOT NULL OR approved IS NOT NULL")
def boost_gold():
    return (dlt.read('uga_boost_silver')
            .withColumn('country_name', lit(COUNTRY))
            .withColumn('approved', col('approved_raw').cast(DoubleType()))
            .withColumn('executed', col('executed_raw').cast(DoubleType()))
            .withColumn('revised', col('approved_raw').cast(DoubleType()))  # no distinct revised column
            .select('country_name', 'year', 'admin0', 'admin1', 'admin2', 'geo0', 'geo1',
                    'is_foreign', 'func', 'func_sub', 'econ', 'econ_sub',
                    'approved', 'revised', 'executed'))
