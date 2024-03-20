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
        if "is_transfer" not in current_df.columns:
            current_df = current_df.withColumn("is_transfer", F.lit(False))
        for col_name in ["func", "func_sub", "admin0", "admin1", "admin2", "geo1", "revised"]:
            if col_name not in current_df.columns:
                current_df = current_df.withColumn(col_name, F.lit(None))

        # TODO: remove once all adm1_name are converted to admin1
        if "adm1_name" not in current_df.columns:
            current_df = current_df.withColumn("adm1_name", F.col("admin1"))
        
        col_order = ['country_name', 'year', 'adm1_name', 'admin0', 'admin1', 'admin2', 'geo1', 'func', 'is_transfer', 'approved', 'revised', 'executed']
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
        .filter(~F.col('is_transfer'))
        .groupBy("country_name", "year").agg(
            F.sum("executed").alias("expenditure"),
            F.sum(
                F.when(boost_gold["adm1_name"] != "Central Scope", boost_gold["executed"])
            ).alias("decentralized_expenditure")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
        .join(year_ranges, on=['country_name'], how='left')
    )

@dlt.table(name=f'expenditure_by_country_adm1_year')
def expenditure_by_country_adm1_year():
    cpi_factors = dlt.read('cpi_factor')
    pop = (spark.table('indicator.subnational_population')
        .select("country_name", "adm1_name", "year", "population")
    )

    year_ranges = (dlt.read('boost_gold')
        .groupBy("country_name", "adm1_name")
        .agg(F.min("year").alias("earliest_year"), 
             F.max("year").alias("latest_year"))
        )
    
    return (dlt.read(f'boost_gold')
        .filter(~F.col('is_transfer'))
        .groupBy("country_name", "adm1_name", "year").agg(F.sum("executed").alias("expenditure"))
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
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
            ).otherwise(
                F.col("adm1_name")
            ))
        .join(year_ranges, on=['country_name', "adm1_name"], how='left')
    )

@dlt.table(name=f'expenditure_by_country_func_year')
def expenditure_by_country_func_year():
    boost_gold = dlt.read('boost_gold')
    year_ranges = (boost_gold
        .groupBy("country_name")
        .agg(F.min("year").alias("earliest_year"),
             F.max("year").alias("latest_year"))
    )
    cpi_factors = dlt.read('cpi_factor')
    
    transfers = (boost_gold
        .filter(F.col('is_transfer'))
        .groupBy("country_name", "year")
        .agg(F.sum("executed").alias("expenditure_transfered"))
        .withColumn('func', F.lit("General public services"))
    )
    return (boost_gold
        .groupBy("country_name", "func", "year")
        .agg(F.sum("executed").alias("expenditure_raw"))
        .join(transfers, on=["country_name", "func", "year"], how="left")
        .withColumn('expenditure', F.col("expenditure_raw")-F.coalesce(F.col("expenditure_transfered"), F.lit(0)))
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
        .join(year_ranges, on=['country_name'], how='left')
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

    return (dlt.read('expenditure_by_country_adm1_year')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(boost_countries, on=['country_name'], how="right")
    )
