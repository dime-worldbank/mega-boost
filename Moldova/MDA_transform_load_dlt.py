# Databricks notebook source
# Moldova BOOST transform + load (DLT).
#
# Emulates the workbook's `Executed`-sheet classification as a PER-LINE pipeline: every microdata
# row gets exactly one econ / econ_sub / func (and a best-effort func_sub). See verification.md for
# the overlap log, the corrected criteria, and the expert sign-off.
#
# Two principles enforced here (both required by the reviewers):
#   1. ORDER-PROOF, NO PRIORITY. Each category is a mutually-exclusive predicate, disjoint *by
#      construction*: every overlap the detector found is resolved by an explicit discriminating
#      criterion baked into the predicate. The `.when()` chains are a readability device only;
#      reordering the branches must not change any row's tag. `boost_silver` asserts this (exactly
#      one econ and one func per row).
#   2. YEAR-AWARE — Moldova has THREE eras, each its own microdata sheet + criteria language:
#        e1 = 2006-2015  English coding   (func1 "06 Education", exp_type "Personnel", ...)
#        e2 = 2016-2019  Romanian coding  (func1 "0900 Invatamint", exp_type "Personal", + econ0)
#        e3 = 2020-2024  Romanian coding  (as e2; a few formulas drop a filter -> see verification.md)
#      The bronze unions the three era CSVs (shared base column names line up; era-only columns are
#      NULL elsewhere) and silver branches on the year.
#
# Defaults implemented here (each is flippable; see verification.md "Decisions"):
#   * econ is an ECONOMIC partition: where a line is matched by both an economic-type category
#     (Wage/Capital/Goods/Subsidies/Interest) and the function-defined Social benefits (e1 only,
#     func1="10 Social care"), the economic-type category WINS and Social benefits keeps only the
#     residual social-care transfers. (verification.md E1-E3)
#   * Goods and services excludes Personal/Capital exp_type in ALL eras — restores the
#     `exp_type<>"Personal"` filter the 2020-24 formula dropped (which double-counts wages/capital
#     into Goods in 2024). (verification.md E0)
#   * econ_sub follows its econ parent, so econ2 "132.11" road-maintenance transfers (which are
#     econ1 "132" => Subsidies) sit in Subsidies to production, not Capital maintenance. (verification.md S1)
#   * func_sub is derived from func2 (self-describing COFOG labels); the func/func_sub ROLLUP
#     hierarchy is left for sign-off (verification.md Q-FS) so func_sub is null where not clearly a leaf.
import dlt
import re
from pyspark.sql.functions import (col, lower, trim, when, lit, substring, regexp_replace,
                                   regexp_extract, coalesce, create_map)
from pyspark.sql.types import DoubleType, IntegerType
from glob import glob

TOP_DIR = "/Volumes/prd_mega/sboost4/vboost4"
WORKSPACE_DIR = f"{TOP_DIR}/Workspace"
COUNTRY = 'Moldova'
COUNTRY_MICRODATA_DIR = f'{WORKSPACE_DIR}/microdata_csv/{COUNTRY}'

CSV_READ_OPTIONS = {"header": "true", "multiline": "true", "quote": '"', "escape": '"'}

# ---- admin1 ground truth ----------------------------------------------------------------------
# The 35 first-level units of Moldova (32 raions + mun. Chisinau + mun. Balti + UTA Gagauzia),
# keyed by the 2-digit CUATM prefix of the locality code. Taken verbatim from the National Bureau
# of Statistics population file `Populatia_sate_comune_orase_2014-2023.xlsx` (columns
# "Municipiu/raion" x "Cod Sat/comuna, oras"); the raion<->code pairing is identical in all ten
# year sheets (2014-2023), so it is treated as the authoritative naming for admin1.
# The BOOST `admin2` label carries the same code zero-padded to 3 digits ("053 Consiliul Raional
# Hincesti"), so the silver layer joins on RAION_BY_ADMIN2_CODE. Diacritics are dropped and the
# statistics office's "R-UL "/"MUN."/"UTA " prefixes stripped, to match the ASCII spelling used
# throughout the workbook. All 35 units are exercised by the microdata; no local code is unmapped.
RAION_BY_CODE = {
    "01": "Chisinau",      "03": "Balti",         "10": "Anenii Noi",   "12": "Basarabeasca",
    "14": "Briceni",       "17": "Cahul",         "21": "Cantemir",     "25": "Calarasi",
    "27": "Causeni",       "29": "Cimislia",      "31": "Criuleni",     "34": "Donduseni",
    "36": "Drochia",       "38": "Dubasari",      "41": "Edinet",       "43": "Falesti",
    "45": "Floresti",      "48": "Glodeni",       "53": "Hincesti",     "55": "Ialoveni",
    "57": "Leova",         "60": "Nisporeni",     "62": "Ocnita",       "64": "Orhei",
    "67": "Rezina",        "71": "Riscani",       "74": "Singerei",     "78": "Soroca",
    "80": "Straseni",      "83": "Soldanesti",    "85": "Stefan Voda",  "87": "Taraclia",
    "89": "Telenesti",     "92": "Ungheni",       "96": "Gagauzia",
}
# ... keyed the way the BOOST admin2 label writes it: 3 digits, leading zero ("01" -> "001").
RAION_BY_ADMIN2_CODE = {f"0{code}": name for code, name in RAION_BY_CODE.items()}

# The three era CSVs already carry logical lowercase headers (year, func1, func2, econ1..econ5,
# exp_type, transfer, econ0, admin1, admin2, approved, revised/adjusted, executed). Nothing to rename;
# we only sanitise any Delta-illegal characters in column names.

# COMMAND ----------

@dlt.expect_or_drop("year_not_null", "year_raw IS NOT NULL")
@dlt.table(name='mda_boost_bronze')
def boost_bronze():
    file_paths = glob(f"{COUNTRY_MICRODATA_DIR}/*.csv")
    df = None
    for f in file_paths:
        part = spark.read.format("csv").options(**CSV_READ_OPTIONS).option("inferSchema", "true").load(f)
        df = part if df is None else df.unionByName(part, allowMissingColumns=True)
    df = df.withColumnRenamed('year', 'year_raw')
    # Delta forbids ' ,;{}()\n\t=' in column names; the logical names are already safe, sanitise the rest.
    for c in df.columns:
        safe = re.sub(r'[ ,;{}()\n\t=]+', '_', c).strip('_')
        if safe != c:
            df = df.withColumnRenamed(c, safe)
    return df


# COMMAND ----------

# NULL-safety: in Spark, `col == x` / `col.startswith(x)` are NULL (not False) when the column is
# NULL, and AND/NOT propagate NULL. The era-specific columns (admin2, econ3, econ5, econ0, ...) are
# blank in the other eras, so every atom is wrapped to return False on NULL via nz().
def nz(c):
    return coalesce(c, lit(False))


def eq(colname, value):
    """Null-safe, case-insensitive equality."""
    return nz(lower(trim(col(colname))) == value.lower())


def sw(colname, prefix):
    """Null-safe, case-insensitive startswith."""
    return nz(lower(trim(col(colname))).startswith(prefix.lower()))


@dlt.table(name='mda_boost_silver')
@dlt.expect("exactly_one_econ", "n_econ = 1")
@dlt.expect("exactly_one_func", "n_func = 1")
def boost_silver():
    # Moldova labels years by the CALENDAR year (microdata `year` = "2006"); no fiscal +1.
    df = (dlt.read('mda_boost_bronze')
          .withColumn('year', substring(trim(col('year_raw')), 1, 4).cast(IntegerType()))
          .filter(col('year').isNotNull()))

    # ---- universe: the workbook Total = transfer "Excluding transfers" (drop inter-budget transfers),
    #      and (2016+) econ0 = "Expenditures" (drop the revenue rows). ----
    keep = (eq('transfer', 'Excluding transfers') | eq('transfer', 'Cu exceptia transferurilor'))
    not_revenue = ~eq('econ0', 'Revenues')          # econ0 blank in e1 -> kept
    df = df.filter(keep & not_revenue)

    e1 = col('year') <= 2015
    e2 = (col('year') >= 2016) & (col('year') <= 2019)
    e3 = col('year') >= 2020
    e23 = col('year') >= 2016

    # ---- admin / geo ----
    # The microdata's own `admin1` column is only a SCOPE flag (Central/Centrale, Local/Locale,
    # Other) -> that becomes admin0. The real first-level unit lives in the e2/e3 `admin2` agency
    # label, which is prefixed with the CUATM raion code ("053 Consiliul Raional Hincesti"). Those
    # labels are dirty -- casing drift ("Consiliul Raional" vs "Consiliul raional"), typos
    # ("Basabareasca"), parentheticals ("Dubasari (Cocieri)"), two bodies per municipality
    # ("Primaria municipiului Chisinau" / "Consiliul municipal Chisinau") and three spellings of
    # Gagauzia -- so admin1 is rebuilt from the CODE alone via RAION_BY_ADMIN2_CODE above.
    #
    # The scope flag MUST be read off a column we do not overwrite: `col('admin1')` is an unresolved
    # reference that Spark re-binds to the LATEST projection, so once admin1 holds the raion name any
    # later `admin1 == 'Locale'` test silently evaluates false (which is what made geo0/geo1 collapse
    # to 'Central' for every local row). Rename it to `admin_scope` first and derive from that.
    df = df.withColumnRenamed('admin1', 'admin_scope')
    is_local = eq('admin_scope', 'Local') | eq('admin_scope', 'Locale')
    admin2_code = regexp_extract(trim(col('admin2')), r'^(\d{3})\s', 1)  # '' when absent (e1)/unprefixed
    raion = create_map([lit(x) for kv in RAION_BY_ADMIN2_CODE.items() for x in kv])[admin2_code]
    df = (df
          .withColumn('admin0', when(is_local, lit('Regional')).otherwise(lit('Central')))
          # NB: admin2 is deliberately left as the RAW agency label and must stay that way -- the e3
          # econ predicates below discriminate on eq('admin2', 'Social Insurance Fund'). Rewriting it
          # here would silently change how those rows are tagged (see the admin_scope note above).
          # admin1 = 'Central Scope' sentinel, or the true raion name from the map -- never a
          # placeholder. Where a local row carries an admin2 label the map cannot resolve, fall back
          # to that RAW label rather than discarding it, so nothing is silently lost. Today that
          # fallback fires on no row: every local admin2 in the workbook is code-prefixed and maps.
          # It only stays NULL when there is no admin2 at all -- i.e. all of e1 (2006-15), whose
          # sheet has no admin2 column, so its region is genuinely unknown.
          .withColumn('admin1', when(is_local, coalesce(raion, col('admin2')))
                                .otherwise(lit('Central Scope')))
          .withColumn('geo0', when(is_local, lit('Regional')).otherwise(lit('Central')))
          # geo1 deliberately reads the REBUILT admin1 (clean raion name, NULL where unknown).
          .withColumn('geo1', when(is_local, col('admin1')).otherwise(lit('Central Scope')))
          # Foreign funding is not separately identified in the Executed sheet (the *_FOR_EXE codes
          # are unpopulated); default False pending a fin_source mapping. See verification.md Q-FF.
          .withColumn('is_foreign', lit(False)))

    # ================= econ atoms (year-aware) =================
    wage = (e1 & eq('exp_type', 'Personnel')) | (e23 & eq('exp_type', 'Personal'))
    capital = (e1 & eq('exp_type', 'Capital')) | (e23 & eq('exp_type', 'Capitale'))
    interest = ((e1 & (sw('econ1', '121') | sw('econ1', '122') | sw('econ1', '123') | sw('econ1', '124')))
                | (e23 & sw('econ2', '240000')))
    subsidies = (e1 & sw('econ1', '132')) | (e23 & sw('econ2', '250000'))
    grants = e23 & sw('econ2', '260000')                  # Other grants/transfers (no e1 equivalent)
    goods = ((e1 & sw('econ1', '113'))
                 | (e2 & sw('econ2', '220000'))
                 | (e3 & sw('econ2', '220000') & ~eq('admin2', 'Social Insurance Fund')))
    # Social benefits: e1 is FUNCTION-defined (func1 "10 Social care") and overlaps the economic
    # types -> economic types win (exclusions below). e2/e3 are econ3-defined (271/272/273) and clean.
    socben = ((e1 & sw('func1', '10 Social care') & ~sw('econ1', '113') & ~eq('exp_type', 'Personnel') & ~eq('exp_type', 'Capital'))
                  | (e2 & (sw('econ3', '271') | sw('econ3', '272') | sw('econ3', '273')))
                  | (e3 & eq('admin2', 'Social Insurance Fund') & (sw('econ3', '271') | sw('econ3', '272'))))

    df = df.withColumn('econ',
        when(wage, 'Wage bill')
        .when(capital, 'Capital expenditures')
        .when(interest, 'Interest on debt')
        .when(subsidies, 'Subsidies')
        .when(grants, 'Other grants and transfers')
        .when(goods, 'Goods and services')
        .when(socben, 'Social benefits')
        .otherwise('Other expenses'))

    # ---- econ_sub (within the econ parent; null where the workbook defines no sub) ----
    pen_con = (e1 & (sw('econ1', '112') | sw('econ1', '116'))) | (e23 & sw('econ3', '212000'))
    cap_main = ((e1 & (sw('econ2', '132.11') | sw('econ2', '243')))
                | (e23 & (sw('econ5', '313120') | sw('econ5', '312120') | sw('econ5', '318120'))))
    goo_bas = ((e1 & (sw('econ2', '113.01') | sw('econ2', '113.04') | sw('econ2', '113.26')
                      | sw('econ2', '113.11') | sw('econ2', '113.19')))
               | (e23 & (sw('econ5', '222110') | sw('econ5', '222120') | sw('econ5', '222300')
                         | sw('econ5', '222220'))))
    goo_emp = (e1 & sw('econ2', '113.16')) | (e23 & sw('econ5', '222930'))
    rec_main = (e1 & sw('econ2', '113.18')) | (e23 & sw('econ5', '222500'))
    soc_assist = ((e2 & (sw('econ3', '272') | sw('econ3', '273')))
                  | (e3 & eq('admin2', 'Social Insurance Fund') & sw('econ3', '272')))
    pensions = ((e2 & sw('econ3', '271'))
                | (e3 & eq('admin2', 'Social Insurance Fund') & sw('econ3', '271')))
    subsidies_production = (~cap_main & (e1 & sw('econ1', '132')) | (e23 & sw('econ2', '250000')))
    df = df.withColumn('econ_sub',
        when(socben & soc_assist, 'Social Assistance')
        .when(socben & pensions, 'Pensions')
        .when(socben, 'Social Assistance')                 # e1 social-care transfers (level not split)
        .when(wage & pen_con, 'Social Benefits (pension contributions)')
        .when(capital & cap_main, 'Capital Maintenance')
        .when(goods & goo_bas, 'Basic Services')
        .when(goods & goo_emp, 'Employment Contracts')
        .when(goods & rec_main, 'Recurrent Maintenance')
        .when(subsidies_production, 'Subsidies to Production')             # S1: 132.11 follows its Subsidies parent
        .otherwise(lit(None).cast('string')))

    # ================= func (10 COFOG, year-aware, disjoint by func1) =================
    # e1: English func1 "NN ..."; e2/e3: COFOG-numbered func1 "0X00"/"10xx". General public services
    # is the residual (.otherwise) — matches the workbook's `Total - SUM(the 9 named functions)`.
    def f1(e1pfx, num):
        return (e1 & sw('func1', e1pfx)) | (e23 & sw('func1', num))

    defense = f1('03 ', '0200')
    pubord = (e1 & (sw('func1', '04 ') | sw('func1', '05 '))) | (e23 & sw('func1', '0300'))
    ecorel = ((e1 & (sw('func1', '11 ') | sw('func1', '13 ') | sw('func1', '14 ') | sw('func1', '16 ')))
              | (e23 & sw('func1', '0400')))
    env = f1('12 ', '0500')
    housing = f1('15 ', '0600')
    health = f1('09 ', '0700')
    rcr = f1('08 ', '0800')
    education = f1('06 ', '0900')
    socpro = (e1 & sw('func1', '10 ')) | (e23 & sw('func1', '10'))

    df = df.withColumn('func',
        when(defense, 'Defence')
        .when(pubord, 'Public order and safety')
        .when(ecorel, 'Economic affairs')
        .when(env, 'Environmental protection')
        .when(housing, 'Housing and community amenities')
        .when(health, 'Health')
        .when(rcr, 'Recreation, culture and religion')
        .when(education, 'Education')
        .when(socpro, 'Social protection')
        .otherwise('General public services'))

    # ---- func_sub (best-effort COFOG leaf from func2; null where not a clear leaf). The func/func_sub
    #      rollup hierarchy is deferred for sign-off (verification.md Q-FS); these leaf tags come
    #      straight from the self-describing func2 labels and are conditioned on the func parent. ----
    f_agr = ((e1 & (sw('func2', '11.01') | sw('func2', '11.02') | sw('func2', '11.03')
                    | sw('func2', '11.05') | sw('func2', '11.10')))
             | (e23 & sw('func2', '0420')))
    f_roads = e1 & sw('func2', '14.07')
    f_rail = e1 & sw('func2', '14.03')
    f_watt = e1 & sw('func2', '14.02')
    f_airt = e1 & sw('func2', '14.04')
    f_transport = (e1 & (sw('func2', '14.01') | sw('func2', '14.09') | sw('func2', '14.10')
                         | sw('func2', '14.08'))) | (e23 & sw('func2', '0450'))
    f_telecom = (e1 & sw('func2', '14.08')) | (e23 & sw('func2', '0460'))
    f_ene_pow = e1 & sw('func2', '16.02')
    f_ene_heat = e1 & sw('func2', '16.03')
    f_ene_oil = e1 & (sw('func2', '16.01') | sw('func2', '16.04'))
    f_energy = (e1 & sw('func2', '16.')) | (e23 & sw('func2', '0430'))
    f_watsan = (e1 & sw('func2', '11.04')) | (e23 & sw('func2', '0630'))
    f_pri_edu = (e1 & (sw('func2', '06.01') | sw('func2', '06.02'))) | (e23 & sw('func2', '0910'))
    f_sec_edu = (e1 & (sw('func2', '06.03') | sw('func2', '06.08'))) | (e23 & (sw('func2', '0920') | sw('func2', '0930')))
    f_ter_edu = (e1 & (sw('func2', '06.04') | sw('func2', '06.05'))) | (e23 & sw('func2', '0940'))

    df = df.withColumn('func_sub',
        # Economic affairs leaves (most specific first)
        when(ecorel & f_roads, 'Roads')
        .when(ecorel & f_rail, 'Railroads')
        .when(ecorel & f_watt, 'Water Transport')
        .when(ecorel & f_airt, 'Air Transport')
        .when(ecorel & f_telecom, 'Telecom')
        .when(ecorel & f_transport, 'Transport')
        .when(ecorel & f_ene_pow, 'Energy (power)')
        .when(ecorel & f_ene_heat, 'Energy (heating)')
        .when(ecorel & f_ene_oil, 'Energy (oil & gas)')
        .when(ecorel & f_energy, 'Energy')
        .when(ecorel & f_agr, 'Agriculture')
        # Housing leaf
        .when(housing & f_watsan, 'Water and Sanitation')
        # Education levels
        .when(education & f_pri_edu, 'Primary Education')
        .when(education & f_sec_edu, 'Secondary Education')
        .when(education & f_ter_edu, 'Tertiary Education')
        .otherwise(lit(None).cast('string')))

    # ---- exclusivity diagnostics for the @dlt.expect checks (the order-proof guarantee) ----
    econ_cats = [wage, capital, interest, subsidies, grants, goods, socben]
    n_econ = sum([c.cast(IntegerType()) for c in econ_cats])
    df = df.withColumn('n_econ', when(n_econ == 0, lit(1)).otherwise(n_econ))   # 0 -> Other expenses
    func_cats = [defense, pubord, ecorel, env, housing, health, rcr, education, socpro]
    n_func = sum([c.cast(IntegerType()) for c in func_cats])
    df = df.withColumn('n_func', when(n_func == 0, lit(1)).otherwise(n_func))   # 0 -> General public services

    return df


# COMMAND ----------

@dlt.table(name='mda_boost_gold')
@dlt.expect_or_drop("executed_or_approved", "executed IS NOT NULL OR approved IS NOT NULL")
def boost_gold():
    return (dlt.read('mda_boost_silver')
            .withColumn('country_name', lit(COUNTRY))
            .withColumn('approved', col('approved').cast(DoubleType()))
            .withColumn('executed', col('executed').cast(DoubleType()))
            # e2 has a 'revised' column; e1/e3 have 'adjusted' -> use whichever is present.
            .withColumn('revised', coalesce(col('revised').cast(DoubleType()),
                                            col('adjusted').cast(DoubleType())))
            .select('country_name', 'year', 'admin0', 'admin1', 'admin2', 'geo0', 'geo1',
                    'is_foreign', 'func', 'func_sub', 'econ', 'econ_sub',
                    'approved', 'revised', 'executed'))
