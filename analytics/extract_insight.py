# Databricks notebook source
# MAGIC %pip install trend-narrative

# COMMAND ----------

from trend_narrative import InsightExtractor
import pandas as pd

CATALOG = "prd_mega"
SCHEMA = "boost"
TABLE_NAME = "expenditure_by_country_func_econ_year"
START_YEAR = 2010

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
MIN_DATA_POINTS = 4

# COMMAND ----------

def process_country(pdf: pd.DataFrame) -> pd.DataFrame:
    """Process all insight configs for a single country."""
    country = pdf["country_name"].iloc[0]
    # Get a fresh "base" slice for the country
    country_base_df = pdf[pdf.year >= START_YEAR]
    insights = []

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

        if len(df_plot) >= MIN_DATA_POINTS:
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

    return pd.DataFrame(insights)


# COMMAND ----------

source_df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE_NAME}")

# Infer schema from a sample country that produces output
sample_countries = source_df.select("country_name").distinct().limit(10).collect()
output_schema = None
for row in sample_countries:
    sample_pdf = source_df.filter(source_df.country_name == row[0]).toPandas()
    sample_output = process_country(sample_pdf)
    if not sample_output.empty:
        output_schema = spark.createDataFrame(sample_output).schema
        break
if output_schema is None:
    raise ValueError("No sample country produced insights - cannot infer schema")

insights_df = source_df.groupBy("country_name").applyInPandas(process_country, schema=output_schema)

# COMMAND ----------

INSIGHT_TABLE_NAME = "expenditure_insights"
insights_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{INSIGHT_TABLE_NAME}")
