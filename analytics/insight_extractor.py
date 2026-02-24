# Databricks notebook source
# Re-exports TrendDetector and InsightExtractor from the shared trend-narrative package.
# Logic now lives in: https://github.com/yukinko-iwasaki/trend-narrative

# COMMAND ----------

# MAGIC %pip install git+https://github.com/yukinko-iwasaki/trend-narrative.git

# COMMAND ----------

from trend_narrative import TrendDetector, InsightExtractor  # noqa: F401
