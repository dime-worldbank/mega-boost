# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'nga', 'tun', 'btn', 'bgd', 'alb', 'ury']

@dlt.table(name=f'boost_gold')
def boost_gold():
    unioned_df = None
    for code in country_codes:
        current_df = spark.table(f'boost_intermediate.{code}_boost_gold')
        for col_name in ["func", "func_sub", "econ", "econ_sub", "admin0", "admin1", "admin2", "geo1", "revised"]:
            if col_name not in current_df.columns:
                current_df = current_df.withColumn(col_name, F.lit(None))

        # Keep adm1_name for easy join with subnational population table
        # Alias geo1 instead of admin1 because considering outcome we care about how much was spent on (not by) a region
        current_df = current_df.withColumn("adm1_name", F.col("geo1"))
        
        col_order = ['country_name', 'year',
                     'adm1_name', 'admin0', 'admin1', 'admin2', 'geo1',
                     'func', 'func_sub', 'econ', 'econ_sub',
                     'approved', 'revised', 'executed']
        current_df = current_df.select(col_order)
            
        if unioned_df is None:
            unioned_df = current_df
        else:
            unioned_df = unioned_df.union(current_df)
    return unioned_df

@dlt.table(name=f'cpi_factor')
def cpi_factor():
    earliest_years = (dlt.read('boost_gold')
        .groupBy("country_name")
        .agg(F.min("year").alias("year"))
    )
    base_cpis = (earliest_years.join(spark.table('indicator.consumer_price_index'), on=["country_name", "year"], how="inner")
        .select(F.col('year').alias('base_cpi_year'),
                'country_name',
                F.col('cpi').alias('base_cpi'))
    )
    return (spark.table('indicator.consumer_price_index')
        .join(base_cpis, on=["country_name"], how="inner")
        .withColumn("cpi_factor", F.col("cpi") / F.col('base_cpi'))
        .select('country_name', "year", "cpi_factor")
    )

@dlt.table(name=f'expenditure_by_country_year')
def expenditure_by_country_year():
    boost_gold = dlt.read('boost_gold')
    year_ranges = (boost_gold
        .groupBy("country_name")
        .agg(F.min("year").alias("earliest_year"), 
             F.max("year").alias("latest_year"))
    )
    cpi_factors = dlt.read('cpi_factor')

    pop = (spark.table('indicator.population')
        .select("country_name", "year", "population"))

    return (boost_gold
        .groupBy("country_name", "year").agg(
            F.sum("executed").alias("expenditure"),
            F.sum(
                F.when(boost_gold["admin0"] == "Regional", boost_gold["executed"])
            ).alias("decentralized_expenditure")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
        .join(pop, on=["country_name", "year"], how="inner")
        .withColumn("per_capita_expenditure", F.col("expenditure") / F.col("population"))
        .withColumn("per_capita_real_expenditure", F.col("real_expenditure") / F.col("population"))
        .join(year_ranges, on=['country_name'], how='left')
    )

# Pre-query for the rpf dash/PowerBI dashboard
@dlt.table(name="pov_expenditure_by_country_year")
def pov_expenditure():
    return (
        spark.table("indicator.poverty").join(
            dlt.read("expenditure_by_country_year"),
            on=["year", "country_name"],
            how="right",
        )
        # Only poor215 is required for the dashboard
        .drop("country_code", "region", "poor365", "poor685", "data_source")
    )

@dlt.table(name=f'expenditure_by_country_geo1_func_year')
def expenditure_by_country_geo1_func_year():
    boost_gold = dlt.read('boost_gold')
    year_ranges = (boost_gold
        .groupBy("country_name")
        .agg(F.min("year").alias("earliest_year"), 
             F.max("year").alias("latest_year"))
    )
    cpi_factors = dlt.read('cpi_factor')

    subnat_pop = spark.table('indicator.subnational_population')
    pop = (subnat_pop.groupBy("country_name", "year")
        .agg(F.sum("population").alias("population"))
        .withColumn("adm1_name", F.lit("Central Scope")) #TODO: update all adm1_name to geo1 after migration off PowerBI
        .union(
            subnat_pop
            .select("country_name", "year", "population", "adm1_name")
        )
    )
    
    return (boost_gold
        .groupBy("country_name", "adm1_name", "func", "year")
        .agg(F.sum("executed").alias("expenditure"))
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
        .join(year_ranges, on=['country_name'], how='left')
        .join(pop, on=["country_name", "adm1_name", "year"], how="inner")
        .withColumn("per_capita_expenditure", F.col("expenditure") / F.col("population"))
        .withColumn("per_capita_real_expenditure", F.col("real_expenditure") / F.col("population"))
        .withColumn("adm1_name_for_map", 
            F.when(
                ((F.col("country_name") == 'Paraguay') & (F.col("adm1_name") == 'Asuncion')),
                F.lit("Asunción")
            ).when(
                ((F.col("country_name") == 'Paraguay')),
                F.concat(F.col("adm1_name"), F.lit(" Department"))
            ).when(
                ((F.col("country_name") == 'Pakistan') & (F.col("adm1_name") == 'Punjab')),
                F.lit("PK-PB")
            ).when(
                ((F.col("country_name") == 'Colombia') & (F.col("adm1_name") == 'Amazonas')),
                F.lit("Amazonas Department Colombia")
            ).when(
                ((F.col("country_name") == 'Colombia')),
                F.concat(F.col("adm1_name"), F.lit(" Department"))
            ).when(
                ((F.col("country_name")=="Kenya") & (F.col("adm1_name")=="Transnzoia")),
                F.lit("Trans-Nzoia County")
            ).when(
                ((F.col("country_name")=="Kenya") & (F.col("adm1_name")=="Murang’A")),
                F.lit("Murang'a County")
            ).when(
                ((F.col("country_name")=="Kenya") & (F.col("adm1_name")=="Tana River")),
                F.lit("Tana River County")
            ).when(
                    F.col('country_name')=='Bhutan', F.concat(F.col("adm1_name"), F.lit(" District, Bhutan"))
            ).when(
                    F.col('country_name')=='Tunisia', F.concat(F.col("adm1_name"), F.lit(" governorate, Tunisia"))
            ).otherwise(
                F.concat(F.col("adm1_name"), F.lit(", "), F.col("country_name"))
            )
        ).withColumn('spent_in_region',
            F.when(
                F.col('adm1_name') == 'Central Scope', 'Unspecified'
            ).otherwise(
                F.lit('Subnational')
            )
        )
    )

@dlt.table(name=f'expenditure_and_outcome_by_country_geo1_func_year')
def expenditure_and_outcome_by_country_geo1_func_year():
    outcome_df = spark.table('indicator.global_data_lab_hd_index')
    exp_window = Window.partitionBy("country_name", "year", "func").orderBy(F.col("per_capita_real_expenditure").desc())
    outcome_window = Window.partitionBy("country_name", "year", "func").orderBy(F.col("outcome_index").desc())

    return (dlt.read('expenditure_by_country_geo1_func_year')
        .join(
            outcome_df, on=["country_name", "adm1_name", "year"], how="inner"
        ).withColumn('outcome_index', 
            F.when(
                F.col('func') == 'Education', F.col('education_index')
            ).when(
                F.col('func') == 'Health', F.col('health_index')
            )
        ).filter(
            F.col('outcome_index').isNotNull()
        ).withColumn("rank_per_capita_real_exp",
            F.rank().over(exp_window)
        ).withColumn("rank_outcome_index",
            F.rank().over(outcome_window)
        )
    )

@dlt.table(name=f'expenditure_by_country_geo1_year')
def expenditure_by_country_geo1_year():
    return (dlt.read(f'expenditure_by_country_geo1_func_year')
        .groupBy("country_name", "adm1_name", "adm1_name_for_map", "year")
        .agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum("per_capita_expenditure").alias("per_capita_expenditure"),
            F.sum("per_capita_real_expenditure").alias("per_capita_real_expenditure"),
            F.min("earliest_year").alias("earliest_year"), 
            F.max("latest_year").alias("latest_year")
        )
    )

@dlt.table(name=f'expenditure_by_country_admin_func_sub_econ_sub_year')
def expenditure_by_country_admin_func_sub_econ_sub_year():
    with_decentralized = (dlt.read('boost_gold')
        .groupBy("country_name", "year", "admin0", "admin1", "admin2", "func", "func_sub", "econ", "econ_sub").agg(
            F.sum("executed").alias("expenditure")
        )
        .join(dlt.read('cpi_factor'), on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
        .filter(F.col("real_expenditure").isNotNull())
    )

    year_ranges = (with_decentralized
        .groupBy("country_name", "func")
        .agg(F.min("year").alias("earliest_year"), 
             F.max("year").alias("latest_year"))
    )

    return with_decentralized.join(year_ranges, on=['country_name', 'func'], how='inner')

@dlt.table(name=f'expenditure_by_country_func_econ_year')
def expenditure_by_country_func_econ_year():
    return (dlt.read('expenditure_by_country_admin_func_sub_econ_sub_year')
        .groupBy("country_name", "year", "func", "econ").agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum(
                F.when(F.col("admin0") == "Regional", F.col("expenditure"))
            ).alias("decentralized_expenditure"),
            F.sum(
                F.when(F.col("admin0") == "Central", F.col("expenditure"))
            ).alias("central_expenditure"),
        )
    )

# This is intentionally not aggregating from expenditure_by_country_geo1_func_year
# because we need decentralized exp which uses admin0
@dlt.table(name=f'expenditure_by_country_func_year')
def expenditure_by_country_func_year():
    pop = (spark.table('indicator.population')
        .select("country_name", "year", "population"))
    
    return (dlt.read('expenditure_by_country_func_econ_year')
        .groupBy("country_name", "year", "func").agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum("decentralized_expenditure").alias("decentralized_expenditure"),
            F.sum("central_expenditure").alias("central_expenditure"),
            F.min("year").alias("earliest_year"), 
            F.max("year").alias("latest_year")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
        .join(pop, on=["country_name", "year"], how="inner")
        .withColumn("per_capita_expenditure", F.col("expenditure") / F.col("population"))
        .withColumn("per_capita_real_expenditure", F.col("real_expenditure") / F.col("population"))
    )

@dlt.table(name=f'expenditure_by_country_econ_year')
def expenditure_by_country_econ_year():
    return (dlt.read('expenditure_by_country_func_econ_year')
        .groupBy("country_name", "year", "econ").agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum("decentralized_expenditure").alias("decentralized_expenditure"),
            F.sum("central_expenditure").alias("central_expenditure"),
            F.min("year").alias("earliest_year"), 
            F.max("year").alias("latest_year")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
    )

@dlt.table(name=f'edu_private_expenditure_by_country_year')
def edu_private_expenditure_by_country_year():
    edu_pub_exp = (dlt.read('expenditure_by_country_func_year')
        .filter(F.col('func') == "Education")
        .select(
            "country_name",
            "year",
            F.col("expenditure").alias("pub_expenditure"),
            F.col("real_expenditure").alias("real_pub_expenditure"),
        )
    )

    cpi_factors = dlt.read('cpi_factor')
    edu_exp = (spark.table('indicator.edu_spending')
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn(
            "real_edu_spending_current_lcu_icp",
            F.col("edu_spending_current_lcu_icp") / F.col("cpi_factor")
        )
    )
    return (
        edu_exp
        .join(edu_pub_exp, on=["country_name", "year"], how="inner")
        .withColumn(
            "expenditure",
            F.col("edu_spending_current_lcu_icp") - F.col("pub_expenditure")
        )
        .withColumn(
            "real_expenditure",
            F.col("real_edu_spending_current_lcu_icp") - F.col("real_pub_expenditure")
        )
        .filter(F.col("expenditure") >= 0)
    )

@dlt.table(name=f'health_private_expenditure_by_country_year')
def health_private_expenditure_by_country_year():
    cpi_factors = dlt.read('cpi_factor')
    return (spark.table('indicator.health_expenditure')
        .withColumn('oop_expenditure_current_lcu', F.col('che') * F.col('oop_percent_che') / 100)
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("oop_expenditure_current_lcu") / F.col("cpi_factor"))
    )

# COMMAND ----------

@dlt.table(name='quality_boost_country')
@dlt.expect_or_fail('country has total agg', 'row_count IS NOT NULL')
def quality_boost_country():
    country_codes_upper = [c.upper() for c in country_codes]
    boost_countries = (spark.table('indicator.country')
        .filter(F.col('country_code').isin(country_codes_upper))
        .select('country_code', 'country_name')
    )
    assert boost_countries.count() == len(country_codes),\
        f'expect all BOOST countries ({country_codes_upper}) to be present in indicator.country table ({boost_countries.select("country_code").collect()})'

    quality_cci_total = (spark.table('boost_intermediate.quality_total_silver')
        .groupBy('country_name')
        .agg(F.count('*').alias('cci_row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )

    return (dlt.read('expenditure_by_country_year')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(quality_cci_total, on=['country_name'], how="right")
    )

@dlt.table(name='quality_boost_subnat')
@dlt.expect_or_fail('country has subnational agg', 'row_count IS NOT NULL')
def quality_boost_subnat():
    no_subnat_countries = ['Uruguay']
    boost_countries = (
        dlt.read('quality_boost_country')
        .filter(~F.col('country_name').isin(no_subnat_countries))
        .select('country_name')
    )

    return (dlt.read('expenditure_by_country_geo1_year')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )

@dlt.table(name='quality_boost_geo1_central_scope')
@dlt.expect_or_fail('country geo1 has central scope', 'row_count IS NOT NULL')
def quality_boost_geo1_central_scope():
    no_geo1_central_scope_countries = ['Uruguay', 'Nigeria']
    boost_countries = (
        dlt.read('quality_boost_country')
        .filter(~F.col('country_name').isin(no_geo1_central_scope_countries))
        .select('country_name')
    )

    return (dlt.read('expenditure_by_country_geo1_year')
        .filter(F.col('adm1_name') == 'Central Scope')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )

@dlt.table(name='quality_boost_admin1_central_scope')
@dlt.expect_or_fail('country admin1 has central scope', 'row_count IS NOT NULL')
def quality_boost_admin1_central_scope():
    boost_countries = dlt.read('quality_boost_country').select('country_name')

    return (dlt.read('boost_gold')
        .filter(F.col('admin1') == 'Central Scope')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )

@dlt.table(name='quality_boost_func')
@dlt.expect_or_fail('country has func agg', 'row_count IS NOT NULL')
def quality_boost_func():
    boost_countries = dlt.read('quality_boost_country').select('country_name')
    quality_cci_func = (spark.table('boost_intermediate.quality_functional_silver')
        .groupBy('country_name', 'func')
        .agg(F.count('*').alias('cci_row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )
    return (
        dlt.read('expenditure_by_country_func_year')
        .groupBy('country_name', 'func')
        .agg(F.count('*').alias('row_count'))
        .join(quality_cci_func, on=['country_name', 'func'], how="right")
    )

@dlt.table(name='quality_boost_func_unknown')
@dlt.expect_or_fail('country has no unknown func', 'cci_row_count IS NOT NULL')
def quality_boost_func_exact():
    boost_countries = dlt.read('quality_boost_country').select('country_name')
    quality_cci_func = (spark.table('boost_intermediate.quality_functional_silver')
        .groupBy('country_name', 'func')
        .agg(F.count('*').alias('cci_row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )
    return (
        dlt.read('expenditure_by_country_func_year')
        .groupBy('country_name', 'func')
        .agg(F.count('*').alias('row_count'))
        .join(quality_cci_func, on=['country_name', 'func'], how="left")
    )

@dlt.table(name='quality_boost_econ')
@dlt.expect_or_fail('country has econ agg', 'row_count IS NOT NULL')
def quality_boost_econ():
    boost_countries = dlt.read('quality_boost_country').select('country_name')
    quality_cci_econ = (spark.table('boost_intermediate.quality_economic_silver')
        .groupBy('country_name', 'econ')
        .agg(F.count('*').alias('cci_row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )
    return (
        dlt.read('expenditure_by_country_econ_year')
        .groupBy('country_name', 'econ')
        .agg(F.count('*').alias('row_count'))
        .join(quality_cci_econ, on=['country_name', 'econ'], how="right")
    )

@dlt.table(name='quality_boost_econ_unknown')
@dlt.expect_or_fail('country has no unknown econ agg', 'cci_row_count IS NOT NULL')
def quality_boost_econ_unknown():
    boost_countries = dlt.read('quality_boost_country').select('country_name')
    quality_cci_econ = (spark.table('boost_intermediate.quality_economic_silver')
        .groupBy('country_name', 'econ')
        .agg(F.count('*').alias('cci_row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )
    return (
        dlt.read('expenditure_by_country_econ_year')
        .groupBy('country_name', 'econ')
        .agg(F.count('*').alias('row_count'))
        .join(quality_cci_econ, on=['country_name', 'econ'], how="left")
    )
