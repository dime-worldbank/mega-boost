# Databricks notebook source
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

# ---- development-region rollup ----------------------------------------------------------------
# admin1 is emitted at DEVELOPMENT-REGION granularity, not raion: the subnational outcome and
# population tables (global_data_lab_hd_index / the population table joined in the cross-country
# aggregate) only carry Moldova at {Chisinau, North, Center, South}, and admin1 is the key used for
# the later outcome-indicator matching. So the 35 raions/municipalities are rolled up to the four
# ADR development regions here. mun. Balti -> North and UTA Gagauzia -> South (the outcome table has
# no separate bucket for either). The full raion name is still recoverable from the raw admin2 label.
# Region names use the EXACT spelling the outcome table uses (North/South/Center/Chisinau).
REGION_BY_RAION = {
    "Chisinau": "Chisinau",
    # North (mun. Balti + 11 raions)
    "Balti": "North",       "Briceni": "North",    "Donduseni": "North",  "Drochia": "North",
    "Edinet": "North",      "Falesti": "North",    "Floresti": "North",   "Glodeni": "North",
    "Ocnita": "North",      "Riscani": "North",    "Singerei": "North",   "Soroca": "North",
    # Center (13 raions)
    "Anenii Noi": "Center", "Calarasi": "Center",  "Criuleni": "Center",  "Dubasari": "Center",
    "Hincesti": "Center",   "Ialoveni": "Center",  "Nisporeni": "Center", "Orhei": "Center",
    "Rezina": "Center",     "Straseni": "Center",  "Soldanesti": "Center","Telenesti": "Center",
    "Ungheni": "Center",
    # South (8 raions + UTA Gagauzia)
    "Basarabeasca": "South","Cahul": "South",      "Cantemir": "South",   "Causeni": "South",
    "Cimislia": "South",    "Leova": "South",      "Stefan Voda": "South","Taraclia": "South",
    "Gagauzia": "South",
}
assert set(REGION_BY_RAION) == set(RAION_BY_CODE.values()), "every raion must map to a region"
# Compose to the key BOOST joins on: the 3-digit admin2 code -> development region.
REGION_BY_ADMIN2_CODE = {code: REGION_BY_RAION[name] for code, name in RAION_BY_ADMIN2_CODE.items()}



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

# Helper functions for defining predicate conditions
def nz(c):
    return coalesce(c, lit(False))


def eq(colname, value):
    """Null-safe, case-insensitive equality."""
    return nz(lower(trim(col(colname))) == value.lower())


def sw(colname, prefix):
    """Null-safe, case-insensitive startswith."""
    return nz(lower(trim(col(colname))).startswith(prefix.lower()))


@dlt.table(name='mda_boost_silver')
def boost_silver():
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
    df = df.withColumnRenamed('admin1', 'admin_scope')
    is_local = eq('admin_scope', 'Local') | eq('admin_scope', 'Locale')
    admin2_code = regexp_extract(trim(col('admin2')), r'^(\d{3})\s', 1)
    # admin1 = the DEVELOPMENT REGION the raion code rolls up to (Chisinau/North/Center/South) --
    # the granularity the outcome & population tables harmonize on. See REGION_BY_ADMIN2_CODE above.
    region = create_map([lit(x) for kv in REGION_BY_ADMIN2_CODE.items() for x in kv])[admin2_code]
    df = (df
          .withColumn('admin0', when(is_local, lit('Regional')).otherwise(lit('Central')))
          # For 2006 to 2015, there is no column `admin2` (blank), so region is NULL for e1.
          .withColumn('admin1', when(is_local, region).otherwise(lit('Central Scope')))
          .withColumn('geo0', when(is_local, lit('Regional')).otherwise(lit('Central')))
          # geo1 follows admin1 (region); this is the adm1_name the cross-country aggregate joins on.
          .withColumn('geo1', when(is_local, col('admin1')).otherwise(lit('Central Scope')))
          # Foreign funding is not separately identified in the Executed sheet 
          .withColumn('is_foreign', lit(False)))


    # ---- econ_sub ----
    pen_con = (e1 & (sw('econ1', '112') | sw('econ1', '116'))) | (e23 & sw('econ3', '212000'))
    cap_main = ((e1 & (sw('econ2', '132.11') | sw('econ2', '243')))
                | (e23 & (sw('econ5', '313120') | sw('econ5', '312120') | sw('econ5', '318120'))))
    goo_bas = ((e1 & (sw('econ2', '113.01') | sw('econ2', '113.04') | sw('econ2', '113.26')
                      | sw('econ2', '113.11') | sw('econ2', '113.19')))
               | (e23 & (sw('econ5', '222110') | sw('econ5', '222120') | sw('econ5', '222300')
                         | sw('econ5', '222220'))))
    goo_emp = (e1 & sw('econ2', '113.16')) | (e23 & sw('econ5', '222930'))
    rec_main = (e1 & sw('econ2', '113.18')) | (e23 & sw('econ5', '222500'))
    
    pensions = ((e1 & (sw('func2','10.01') | sw('func2', '10.21')) & sw('econ1', '135'))|(e2 & sw('econ3', '271'))
                | (e3 & eq('admin2', 'Social Insurance Fund') & sw('econ3', '271')))
    
    soc_assist = (~pensions & ((e1 & sw('func1', '10 Social care') & ~sw('econ1', '113')) & ~sw('econ1', '112') & ~sw('econ1', '116')& ~eq('exp_type', 'Personnel') & ~eq('exp_type', 'Capital'))
                    | (e2 & (sw('econ3', '272') | sw('econ3', '273')))
                    | (e3 & eq('admin2', 'Social Insurance Fund') & sw('econ3', '272000')))
    
    subsidies_production = (~cap_main & (e1 & sw('econ1', '132') & ~sw('econ2','132.11')) | (e23 & sw('econ2', '250000')))

    # ================= econ atoms (year-aware) =================
    wage = (e1 & eq('exp_type', 'Personnel')) | (e23 & eq('exp_type', 'Personal'))
    capital = (e1 & eq('exp_type', 'Capital')) | (e23 & eq('exp_type', 'Capitale'))
    interest = ((e1 & (sw('econ1', '121') | sw('econ1', '122') | sw('econ1', '123') | sw('econ1', '124')))
                | (e23 & sw('econ2', '240000')))
    subsidies = (e1 & sw('econ1', '132') & ~sw('econ2','132.11')) | (e23 & sw('econ2', '250000'))
    grants = e23 & sw('econ2', '260000')                  # Other grants/transfers (no e1 equivalent)
    goods = ((e1 & sw('econ1', '113'))
                 | (e2 & sw('econ2', '220000') & ~sw('exp_type', 'Personal'))
                 | (e3 & sw('econ2', '220000') & ~eq('admin2', 'Social Insurance Fund')))
    socben = soc_assist | pensions
    

    df = df.withColumn('econ',
        when(wage, 'Wage bill')
        .when(capital, 'Capital expenditures')
        .when(interest, 'Interest on debt')
        .when(subsidies, 'Subsidies')
        .when(grants, 'Other grants and transfers')
        .when(goods, 'Goods and services')
        .when(socben, 'Social benefits')
        .otherwise('Other expenses'))
    
    df = df.withColumn('econ_sub',
        when(soc_assist, 'Social Assistance')
        .when(pensions, 'Pensions')
        .when(pen_con, 'Social Benefits (pension contributions)')
        .when(cap_main, 'Capital Maintenance')
        .when(goo_bas, 'Basic Services')
        .when(goo_emp, 'Employment Contracts')
        .when(rec_main, 'Recurrent Maintenance')
        .when(subsidies_production, 'Subsidies to Production')
        .otherwise(lit(None).cast('string')))

    # ---- func_sub (COFOG leaf from func2; null where not a clear leaf) ----
    f_agr = ((e1 & (sw('func1', '11 Agriculture, forestry, fishery and water service')))
             | (e23 & sw('func2', '0420')))
    f_roads = e1 & sw('func2', '14.07')
    f_watt = (e1 & sw('func2', '14.02')) | (e23 & sw('func3', '0452'))

    f_transport = (e1 & sw('func1', '14')) & ~sw('func2', '14.08') | (e23 & sw('func2', '0450'))
    f_telecom = (e1 & sw('func2', '08.03')) | (e23 & sw('func3', '0831')& sw('econ1', '300000'))
    f_energy = (e1 & sw('func1', '16')) | (e23 & sw('func2', '0430'))  
    f_pri_edu = (e1 & (sw('func2', '06.01') | sw('func2', '06.02'))) | (e23 & sw('func2', '0910'))
    f_sec_edu = (e1 & (sw('func2', '06.03') | sw('func2', '06.08'))) | (e23 & (sw('func2', '0920')))
    f_ter_edu = (e1 & (sw('func2', '04') | sw('func2', '06.05'))) | (e23 & sw('func2', '0940'))  

    f_judice = (e1 & sw('func1', '04')) | (e23 & sw('func2', '0330'))
    f_public_safety = ~f_judice & (e1 & sw('func1', '05')) | (e23 & sw('func1', '0300'))

    # ================= func  =================
    def f1(e1pfx, num):
        return (e1 & sw('func1', e1pfx)) | (e23 & sw('func1', num))

    defense = f1('03 ', '0200')
    pubord = f_judice | f_public_safety
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


    df = df.withColumn('func_sub',
        # Economic affairs leaves
        when(f_roads, 'Roads')
        .when(f_watt, 'Water Transport')
        .when( f_telecom, 'Telecom')
        .when( f_transport, 'Transport')
        .when( f_energy, 'Energy')
        .when(f_agr, 'Agriculture')
        # Education levels
        .when(f_pri_edu, 'Primary Education')
        .when(f_sec_edu, 'Secondary Education')
        .when(f_ter_edu, 'Tertiary Education')
        .otherwise(lit(None).cast('string')))

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
