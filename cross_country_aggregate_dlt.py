# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'nga', 'tun', 'btn']

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
        .join(year_ranges, on=['country_name'], how='left')
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
        .withColumn("adm1_name", F.lit("Central Scope"))
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
                F.col("adm1_name")
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

# This is intentionally not aggregating from expenditure_by_country_geo1_func_year
# because we need decentralized exp which uses admin0
@dlt.table(name=f'expenditure_by_country_func_year')
def expenditure_by_country_func_year():
    return (dlt.read('expenditure_by_country_admin_func_sub_econ_sub_year')
        .groupBy("country_name", "year", "func").agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum(
                F.when(F.col("admin0") == "Regional", F.col("expenditure"))
            ).alias("decentralized_expenditure"),
            F.sum(
                F.when(F.col("admin0") == "Central", F.col("expenditure"))
            ).alias("central_expenditure"),
            F.min("earliest_year").alias("earliest_year"), 
            F.max("latest_year").alias("latest_year")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
    )


@dlt.table(name=f'edu_private_expenditure_by_country_year')
def edu_private_expenditure_by_country_year():
    cpi_factors = dlt.read('cpi_factor')
    return (spark.table('indicator.edu_private_spending')
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("edu_private_spending_current_lcu") / F.col("cpi_factor"))
    )

@dlt.table(name=f'health_private_expenditure_by_country_year')
def health_private_expenditure_by_country_year():
    cpi_factors = dlt.read('cpi_factor')
    return (spark.table('indicator.health_expenditure')
        .withColumn('oop_expenditure_current_lcu', F.col('che') * F.col('oop_percent_che') / 100)
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("oop_expenditure_current_lcu") / F.col("cpi_factor"))
    )

@dlt.table(name='quality_boost_agg')
@dlt.expect_or_fail('country has subnational agg', 'row_count IS NOT NULL')
def quality_exp_by_country_year():
    country_codes_upper = [c.upper() for c in country_codes]
    boost_countries = (spark.table('indicator.country')
        .filter(F.col('country_code').isin(country_codes_upper))
        .select('country_code', 'country_name')
    )
    assert boost_countries.count() == len(country_codes),\
        f'expect all BOOST countries ({country_codes_upper}) to be present in indicator.country table ({boost_countries.select("country_code").collect()})'

    return (dlt.read('expenditure_by_country_geo1_year')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )
