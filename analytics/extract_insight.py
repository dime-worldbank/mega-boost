# Databricks notebook source
# MAGIC %run ./insight_extractor

# COMMAND ----------

CATALOG = "prd_mega"
SCHEMA = "boost"
TABLE_NAME = "expenditure_by_country_func_econ_year"
START_YEAR = 2010
source_table = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE_NAME}")
source_table = source_table.toPandas()
countries = source_table.country_name.unique()
INSIGHT_CONFIGS = [
    {
        "dimension": "func",
        "dimension_filter": "Health",
        "metric": "real_expenditure",
        "metric_name": "real expenditure",
    },
    {
        "dimension": "func",
        "dimension_filter": "Education",
        "metric": "real_expenditure",
        "metric_name": "real expenditure",
    },
    {
        "dimension": None,
        "dimension_filter": None,
        "metric": "real_expenditure",
        "metric_name": "total real expenditure",
    },
]

# countries = source_table.country_name.unique()
insights = []

for country in countries:
    # Get a fresh "base" slice for the country
    country_base_df = source_table[
        (source_table.country_name == country) & (source_table.year >= START_YEAR)
    ]

    for config in INSIGHT_CONFIGS:
        metric = config["metric"]
        dim = config["dimension"]
        dim_val = config["dimension_filter"]

        # Start fresh with the country's data for this specific config
        df_temp = country_base_df.copy().dropna(subset=[metric])

        # Apply dimension filter if it exists (e.g., Health)
        if dim and dim_val:
            df_temp = df_temp[df_temp[dim] == dim_val]

        # Aggregate to Year level (summing up expenditures)
        df_plot = df_temp.groupby("year")[metric].sum().reset_index()
        df_plot = df_plot.sort_values(by="year")

        if len(df_plot) >= 4:
            X = df_plot["year"].values
            Y = df_plot[metric].values

            # Pure math extraction
            extractor = InsightExtractor(X, Y)
            result = extractor.extract_full_suite()

            # Explicit Assignment (Metadata)
            result.update(
                {
                    "country_name": country,
                    "metric": metric,
                    "metric_name": config["metric_name"],
                    "dimension": dim if dim else "Total",
                    "dimension_filter": dim_val if dim_val else "Total",
                    "table_name": TABLE_NAME,
                }
            )
            insights.append(result)

# COMMAND ----------

INSIGHT_TABLE_NAME = 'expenditure_insights'
insights_df = pd.DataFrame(insights)
sdf = spark.createDataFrame(insights_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{INSIGHT_TABLE_NAME}")
