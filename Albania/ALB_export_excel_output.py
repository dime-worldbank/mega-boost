# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import openpyxl
import tempfile
import shutil

output_path = OUTPUT_DIR + "albania.xlsx"
raw_data =  spark.table(f"{TARGET}.alb_publish_test").toPandas()

# Databricks Volumes do not support direct writing of binary files (e.g., Excel) from Python libraries.
# Workaround: write to a local temporary file, then copy it to the volume.
with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=True) as tmp:
    temp_path = tmp.name
    raw_data.to_excel(temp_path,engine="openpyxl", index=False)
    print("Excel sheet saved to local temporary file")
    shutil.copy(temp_path, output_path)
print(f"Excel file saved to: {output_path}")
