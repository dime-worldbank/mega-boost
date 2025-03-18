# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

catalog = 'prd_mega'
indicator_schema = 'indicator'
boost_intermediate_schema = 'boost_intermediate'

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'nga', 'tun', 'btn', 'bgd', 'alb', 'ury', "zaf", 'chl', 'gha']

schema = StructType([
    StructField("country_name", StringType(), True, {'comment': 'The name of the country for which the budget data is recorded (e.g., "Kenya", "Brazil").'}),
    StructField("year", IntegerType(), True, {'comment': 'The fiscal year for the budget data (e.g., 2023, 2024).'}),
    StructField("admin0", StringType(), True, {'comment': 'Who spent the money at the highest administrative level (either "Central" or "Regional").'}),
    StructField("admin1", StringType(), True, {'comment': 'Who spent the money at the first sub-national administrative level (e.g., state or province name).'}),
    StructField("admin2", StringType(), True, {'comment': 'Who spent the money at the second sub-national administrative level (e.g., government agency/ministry name or district).'}),
    StructField("geo0", StringType(), True, {'comment': 'Is the money geographically or centrally allocated (either "Central" or "Regional") regardless of the spender'}),
    StructField("geo1", StringType(), True, {'comment': 'Geographically, at which first sub-national administrative level the money was spent.'}),
    StructField("adm1_name", StringType(), True, {'comment': 'Legacy alias for geo1'}),
    StructField("func", StringType(), True, {'comment': 'Functional classification of the budget (e.g., Health, Education).'}),
    StructField("func_sub", StringType(), True, {'comment': 'Sub-functional classification under the main COFOG function (e.g., primary education, secondary education).'}),
    StructField("econ", StringType(), True, {'comment': 'Economic classification of the budget (e.g., Wage bill, Goods and services).'}),
    StructField("econ_sub", StringType(), True, {'comment': 'Sub-economic classification under the main economic category (e.g., basic wages, allowances).'}),
    StructField("is_foreign", BooleanType(), True, {'comment': 'Indicator whether the expenditure is foreign funded or not.'}),
    StructField("approved", DoubleType(), True, {'comment': 'Amount of budget in current local currency approved by the relevant authority.'}),
    StructField("revised", DoubleType(), True, {'comment': 'Revised budget in current local currency amount during the fiscal year.'}),
    StructField("executed", DoubleType(), True, {'comment': 'Actual amount spent in current local currency by the end of the fiscal year.'})
])

@dlt.expect_or_drop("executed/approved not null nor 0", "(executed IS NOT NULL AND executed != 0) OR (approved IS NOT NULL AND approved != 0)")
@dlt.table(
    name='boost_gold',
    comment="This dataset includes BOOST budget and expenditure data for multiple countries across various years, with information presented at the most granular level possible to ensure maximum analytical flexibility. Each entry is harmonized to include standardized labels for common BOOST features, such as functional and economic categories.",
    schema=schema
)
def boost_gold():
    unioned_df = None
    for code in country_codes:
        table_name = f'{catalog}.{boost_intermediate_schema}.{code}_boost_gold'
        current_df = spark.table(table_name)
        for col_name in ["func", "func_sub", "econ", "econ_sub", "admin0", "admin1", "admin2", "geo1", "revised", "is_foreign"]:
            if col_name not in current_df.columns:
                current_df = current_df.withColumn(col_name, F.lit(None))

        for col_name in ["approved", "executed"]:
            assert col_name in current_df.columns, f"Column '{col_name}' must be present in the table '{table_name}"
            col_type = current_df.schema[col_name].dataType
            assert isinstance(col_type, DoubleType), \
                f"Table '{table_name} column '{col_name}' must be a DoubleType, but got {col_type}"

        # Keep adm1_name for easy join with subnational population table
        # Alias geo1 instead of admin1 because considering outcome we care about how much was spent on (not by) a region
        current_df = (current_df
            .withColumn("adm1_name", F.col("geo1"))
            .withColumn("geo0",
                F.when((F.col("geo1") == "Central Scope") | (F.col("geo1").isNull()), F.lit("Central"))
                .otherwise(F.lit("Regional"))
            )
        )

        col_order = [
            'country_name', 'year',
            'admin0', 'admin1', 'admin2', 'geo0', 'geo1', 'adm1_name',
            'func', 'func_sub', 'econ', 'econ_sub', 'is_foreign',
            'approved', 'revised', 'executed'
        ]
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
    base_cpis = (earliest_years.join(spark.table(f'{catalog}.{indicator_schema}.consumer_price_index'), on=["country_name", "year"], how="inner")
        .select(F.col('year').alias('base_cpi_year'),
                'country_name',
                F.col('cpi').alias('base_cpi'))
    )
    return (spark.table(f'{catalog}.{indicator_schema}.consumer_price_index')
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

    pop = (spark.table(f'{catalog}.{indicator_schema}.population')
        .select("country_name", "year", "population"))

    return (boost_gold
        .groupBy("country_name", "year").agg(
            F.sum("executed").alias("expenditure"),
            F.sum(
                F.when(boost_gold["admin0"] == "Regional", boost_gold["executed"])
            ).alias("decentralized_expenditure"),
            F.sum(
                F.when(boost_gold["is_foreign"], boost_gold["executed"])
            ).alias("foreign_funded_expenditure")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
        .withColumn("expenditure_foreign_ratio",
            F.col("foreign_funded_expenditure") / F.col("expenditure")
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
        spark.table(f"{catalog}.{indicator_schema}.poverty").join(
            dlt.read("expenditure_by_country_year"),
            on=["year", "country_name"],
            how="right",
        )
        # Only poor215 is required for the dashboard
        .drop("country_code", "region", "poor365", "poor685", "data_source")
    )

@dlt.table(name='expenditure_by_country_geo1_func_year')
def expenditure_by_country_geo1_func_year():
    boost_gold = dlt.read('boost_gold')
    year_ranges = (boost_gold
        .groupBy("country_name")
        .agg(F.min("year").alias("earliest_year"), 
             F.max("year").alias("latest_year"))
    )
    cpi_factors = dlt.read('cpi_factor')

    subnat_pop = spark.table(f'{catalog}.{indicator_schema}.subnational_population')
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
    outcome_df = spark.table(f'{catalog}.{indicator_schema}.global_data_lab_hd_index')
    exp_window = Window.partitionBy("country_name", "year", "func").orderBy(F.col("per_capita_real_expenditure").desc())
    outcome_window = Window.partitionBy("country_name", "year", "func").orderBy(F.col("outcome_index").desc())
    geo1_func_df = dlt.read('expenditure_by_country_geo1_func_year')

    ranked_df = (geo1_func_df
        .join(
            outcome_df, on=["country_name", "adm1_name", "year"], how="inner"
        ).withColumn('outcome_index', 
            F.when(
                F.col('func') == 'Education', F.col('attendance_6to17yo')
            ).when(
                F.col('func') == 'Health', F.col('health_index')
            )
        ).filter(
            F.col('outcome_index').isNotNull()
        ).withColumn("rank_per_capita_real_exp",
            F.rank().over(exp_window)
        ).withColumn("rank_outcome_index",
            F.rank().over(outcome_window)
        ).select(
            "country_name",
            "adm1_name",
            "year",
            "func",
            "outcome_index",
            "rank_per_capita_real_exp",
            "rank_outcome_index",
        )
    )

    return geo1_func_df.join(ranked_df, on=["country_name", "adm1_name", "year", "func"], how="left")

@dlt.table(name=f'expenditure_by_country_geo1_year')
def expenditure_by_country_geo1_year():
    return (dlt.read('expenditure_by_country_geo1_func_year')
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
        .withColumn("is_foreign", F.when(F.col("is_foreign").isNull(), False).otherwise(F.col("is_foreign")))
        .groupBy("country_name", "year", "admin0", "admin1", "admin2", "func", "func_sub", "econ", "econ_sub", "is_foreign").agg(
            F.sum("executed").alias("expenditure"),
            F.sum("approved").alias("approved")
        )
        .join(dlt.read(f'cpi_factor'), on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("expenditure") / F.col("cpi_factor"))
    )

    year_ranges = (with_decentralized
        .groupBy("country_name", "func")
        .agg(F.min("year").alias("earliest_year"), 
             F.max("year").alias("latest_year"))
    )

    return with_decentralized.join(year_ranges, on=['country_name', 'func'], how='inner')

@dlt.table(name='expenditure_by_country_geo0_func_sub_year')
def expenditure_by_country_geo0_func_sub_year():
    with_decentralized = (dlt.read('boost_gold')
        .groupBy("country_name", "year", "geo0", "func", "func_sub").agg(
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
    pop = (spark.table(f'{catalog}.{indicator_schema}.population')
        .select("country_name", "year", "population"))
    
    return (dlt.read('expenditure_by_country_admin_func_sub_econ_sub_year')
        .groupBy("country_name", "year", "func", "econ")
        .agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum(
                F.when(F.col("admin0") == "Regional", F.col("expenditure"))
            ).alias("decentralized_expenditure"),
            F.sum(
                F.when(F.col("admin0") == "Central", F.col("expenditure"))
            ).alias("central_expenditure"),
            F.sum(
                F.when(~F.col("is_foreign"), F.col("approved"))
            ).alias("domestic_funded_budget")
        )
        .join(pop, on=["country_name", "year"], how="inner")
        .withColumn("per_capita_expenditure", F.col("expenditure") / F.col("population"))
        .withColumn("per_capita_real_expenditure", F.col("real_expenditure") / F.col("population"))
    )

# This is intentionally not aggregating from expenditure_by_country_geo1_func_year
# because we need decentralized exp which uses admin0
@dlt.table(name=f'expenditure_by_country_func_year')
def expenditure_by_country_func_year():
    return (dlt.read(f'expenditure_by_country_func_econ_year')
        .groupBy("country_name", "year", "func").agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum("decentralized_expenditure").alias("decentralized_expenditure"),
            F.sum("central_expenditure").alias("central_expenditure"),
            F.sum("per_capita_expenditure").alias("per_capita_expenditure"),
            F.sum("per_capita_real_expenditure").alias("per_capita_real_expenditure"),
            F.min("population").alias("population"),
            F.min("year").alias("earliest_year"), 
            F.max("year").alias("latest_year")
        )
        .withColumn("expenditure_decentralization",
            F.col("decentralized_expenditure") / F.col("expenditure")
        )
    )

@dlt.table(name=f'expenditure_by_country_econ_year')
def expenditure_by_country_econ_year():
    return (dlt.read('expenditure_by_country_func_econ_year')
        .groupBy("country_name", "year", "econ").agg(
            F.sum("expenditure").alias("expenditure"),
            F.sum("real_expenditure").alias("real_expenditure"),
            F.sum("decentralized_expenditure").alias("decentralized_expenditure"),
            F.sum("central_expenditure").alias("central_expenditure"),
            F.sum("per_capita_expenditure").alias("per_capita_expenditure"),
            F.sum("per_capita_real_expenditure").alias("per_capita_real_expenditure"),
            F.min("population").alias("population"),
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
    edu_exp = (spark.table(f'{catalog}.{indicator_schema}.edu_spending')
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
    return (spark.table(f'{catalog}.{indicator_schema}.health_expenditure')
        .withColumn('oop_expenditure_current_lcu', F.col('che') * F.col('oop_percent_che') / 100)
        .join(cpi_factors, on=["country_name", "year"], how="inner")
        .withColumn("real_expenditure", F.col("oop_expenditure_current_lcu") / F.col("cpi_factor"))
    )

# COMMAND ----------

excluded_country_year_conditions = (
    (F.col('country_name') == 'Burkina Faso') & (F.col('year') == 2016) |
    (F.col('country_name') == 'Bangladesh') & (F.col('year') == 2008) |
    (F.col('country_name') == 'Kenya') & (F.col('year').isin(list(range(2006, 2016)))) |
    (F.col('country_name') == 'Chile') & (F.col('year') < 2009)
)

@dlt.table(name='quality_boost_country')
@dlt.expect_or_fail('country has total agg for year', 'expenditure IS NOT NULL')
def quality_boost_country():
    country_codes_upper = [c.upper() for c in country_codes]
    boost_countries = (spark.table(f'{catalog}.{indicator_schema}.country')
        .filter(F.col('country_code').isin(country_codes_upper))
        .select('country_code', 'country_name')
    )
    assert boost_countries.count() == len(country_codes),\
        f'expect all BOOST countries ({country_codes_upper}) to be present in {catalog}.{indicator_schema}.country table ({boost_countries.select("country_code").collect()})'

    quality_cci_total = (spark.table(f'{catalog}.{boost_intermediate_schema}.quality_total_gold')
        .filter(F.col('approved_or_executed') == 'Executed')
        .filter(~excluded_country_year_conditions)
        .join(boost_countries, on=['country_name'], how="right")
    )

    return (dlt.read('expenditure_by_country_year')
        .join(quality_cci_total, on=['country_name', 'year'], how="right")
    )

@dlt.table(name='quality_boost_subnat')
@dlt.expect_or_fail('country has subnational agg', 'row_count IS NOT NULL')
def quality_boost_subnat():
    no_subnat_countries = ['Uruguay']
    boost_countries = (
        dlt.read('quality_boost_country')
        .filter(~F.col('country_name').isin(no_subnat_countries))
        .select('country_name')
        .distinct()
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
        .distinct()
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
    boost_countries = dlt.read('quality_boost_country').select('country_name').distinct()

    return (dlt.read('boost_gold')
        .filter(F.col('admin1') == 'Central Scope')
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(boost_countries, on=['country_name'], how="right"))

@dlt.table(name='quality_boost_func')
@dlt.expect_or_fail('country has func agg for year', 'expenditure IS NOT NULL')
def quality_boost_func():
    boost_countries = dlt.read('quality_boost_country').select('country_name').distinct()
    quality_cci_func = (spark.table(f'{catalog}.{boost_intermediate_schema}.quality_functional_gold')
        .filter(F.col('approved_or_executed') == 'Executed')
        .filter(~excluded_country_year_conditions)
        .join(boost_countries, on=['country_name'], how="right")
    )
    return (
        dlt.read('expenditure_by_country_func_year')
        .join(quality_cci_func, on=['country_name', 'func', 'year'], how="right")
    )

@dlt.table(name='quality_boost_func_unknown')
@dlt.expect_or_fail('country has no unknown func', 'cci_row_count IS NOT NULL')
def quality_boost_func_exact():
    # This doesn't check by year on purpose as new years may be added to pipeline
    # without the CCI excel being updated.
    boost_countries = dlt.read('quality_boost_country').select('country_name').distinct()
    quality_cci_func = (spark.table(f'{catalog}.{boost_intermediate_schema}.quality_functional_gold')
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
@dlt.expect_or_fail('country has econ agg for year', 'expenditure IS NOT NULL')
def quality_boost_econ():
    boost_countries = dlt.read('quality_boost_country').select('country_name').distinct()
    quality_cci_econ = (spark.table(f'{catalog}.{boost_intermediate_schema}.quality_economic_gold')
        .filter(F.col('approved_or_executed') == 'Executed')
        .filter(~excluded_country_year_conditions)
        .join(boost_countries, on=['country_name'], how="right")
    )
    return (
        dlt.read('expenditure_by_country_econ_year')
        .join(quality_cci_econ, on=['country_name', 'econ', 'year'], how="right")
    )

@dlt.table(name='quality_boost_econ_unknown')
@dlt.expect_or_fail('country has no unknown econ agg', 'cci_row_count IS NOT NULL')
def quality_boost_econ_unknown():
    boost_countries = dlt.read('quality_boost_country').select('country_name').distinct()
    quality_cci_econ = (spark.table(f'{catalog}.{boost_intermediate_schema}.quality_economic_gold')
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

@dlt.table(name='quality_boost_foreign')
@dlt.expect_or_fail('country has foreign agg', 'row_count IS NOT NULL')
def quality_boost_foreign():
    boost_countries = dlt.read('quality_boost_country').select('country_name').distinct()
    quality_cci_foreign = (spark.table(f'{catalog}.{boost_intermediate_schema}.quality_total_foreign_gold')
        .groupBy('country_name')
        .agg(F.count('*').alias('cci_row_count'))
        .join(boost_countries, on=['country_name'], how="inner")
    )
    return (
        dlt.read('expenditure_by_country_year')
        .filter(F.col('foreign_funded_expenditure').isNotNull())
        .groupBy('country_name')
        .agg(F.count('*').alias('row_count'))
        .join(quality_cci_foreign, on=['country_name'], how="right")
    )
