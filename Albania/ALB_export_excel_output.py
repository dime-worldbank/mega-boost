# Databricks notebook source
# MAGIC %run ../utils
# MAGIC

# COMMAND ----------

import logging
import tempfile
import shutil

from pyspark.sql.functions import col, sum as _sum, lit
from pyspark.sql import functions as F

import pandas as pd
from openpyxl import load_workbook


OUTPUT_FILE_PATH = f"{OUTPUT_DIR}/Albania_BOOST.xlsx"
SOURCE_FILE_PATH = f"{INPUT_DIR}/Albania BOOST.xlsx"
BOOST_TARGET = 'prd_mega.boost_intermediate'

# COMMAND ----------


# Load and filter
raw_data = (
    spark.table(f"{BOOST_TARGET}.alb_boost_gold")
    .withColumn("year", col("year").cast("string"))
    .cache()
)

required_columns = [
    "admin0",
    "admin1",
    "admin2",
    "econ",
    "econ_sub",
    "func",
    "func_sub",
    "is_foreign",
    "approved",
    "executed",
    "year"
]

for col_name in required_columns:
    if col_name not in raw_data.columns:
        raise KeyError(f"Required column '{col_name}' is missing in the DataFrame.")

tag_code_mapping = pd.read_csv(f"{AUXI_DIR}/tag_label_mapping.csv") 
years = [str(year) for year in sorted(raw_data.select("year").distinct().rdd.flatMap(lambda x: x).collect())]

def create_pivot(df, parent, child, agg_col, central=True, ):
    # Filter based on admin0 level
    if central:
        df = df.filter(F.col("admin0") == "Central")
        filtered_mapping = tag_code_mapping[tag_code_mapping['subnational'].isna()]
    else:
        df = df.filter(F.col("admin0") == "Regional")
        filtered_mapping = tag_code_mapping[~tag_code_mapping['subnational'].isna()]

    # Step 1: Get detailed level (econ + econ_sub + year)
    detailed = (
        df.groupBy(parent, child, "year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumnRenamed(parent, "parent")
        .withColumnRenamed(child, "child")
    )

    # Step 2: Get subtotals at parent level (econ + year), econ_sub = 'Subtotal'
    subtotals = (
        df.groupBy(parent, "year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumn("child", F.lit("Subtotal"))  # Ensure same schema
        .withColumnRenamed(parent, "parent")
    )

    # Step 3: Union both
    combined = detailed.unionByName(subtotals)

    # Step 4: Pivot to wide format
    pivoted = (
        combined.groupBy("parent", "child")
        .pivot("year")
        .agg(F.sum(agg_col))
        .fillna(0)  # Replace NaNs with 0
    )

    # Step 5: Join with filtered_mapping
    filtered_mapping_spark = spark.createDataFrame(filtered_mapping)
    result = (
        pivoted.join(filtered_mapping_spark, on=["parent", "child"])
        .drop("Categories", "parent", "child", "parent_type", "child_type", "subnational")
    )

    total_classifications = filtered_mapping_spark.select("Code").distinct().rdd.flatMap(lambda x: x).collect()
    total_matches = result.select("Code").distinct().rdd.flatMap(lambda x: x).collect()
    logging.info(f"Matched entries for {parent}, {child}: {len(total_matches)} / {len(total_classifications)}")

    return result


# COMMAND ----------

def create_pivot_total(df, agg_col, central=True):
    # Filter based on admin0 level
    if central:
        df = df.filter(F.col("admin0") == "Central")
    else:
        df = df.filter(F.col("admin0") == "Regional")

    # Step 1: Calculate total expenditure by year
    total = (
        df.groupBy("year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumn("Code", F.lit("EXP_ECON_TOT_EXP_EXE"))
    )

    # Step 2: Calculate foreign expenditure by year
    foreign_total = (
        df.filter(F.col("is_foreign") == True)
        .groupBy("year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumn("Code", F.lit("EXP_ECON_TOT_EXP_FOR_EXE"))
    )

    # Step 3: Combine total and foreign expenditure
    combined = total.unionByName(foreign_total)

    # Step 4: Pivot to wide format
    pivoted = (
        combined.groupBy("Code")
        .pivot("year")
        .agg(F.first(agg_col))
        .fillna(0)  # Replace NaNs with 0
    )
    return pivoted

# COMMAND ----------

pairs = [('econ', 'econ_sub'), ('func', 'econ_sub'), ('func', 'econ'), ('func', 'func_sub'), ('func_sub', 'econ'), ('func_sub', 'econ_sub')]

def generate_combined_pivots(pairs, agg_col):
    # Initialize an empty DataFrame for combining results
    combined = None

    for parent, child in pairs:
        # Create pivot for central and regional levels
        pivoted = create_pivot(raw_data, parent, child,agg_col, central=True)
        pivoted_subnational = create_pivot(raw_data, parent, child,agg_col, central=False)
        
        # Combine the results
        if combined is None:
            combined = pivoted.unionByName(pivoted_subnational)
        else:
            combined = combined.unionByName(pivoted).unionByName(pivoted_subnational)

    # Add totals to the combined DataFrame
    totals = create_pivot_total(raw_data, agg_col)
    combined = combined.unionByName(totals)
    totals_subnational = create_pivot_total(raw_data, agg_col, central=False)
    combined = combined.unionByName(totals_subnational)
    return combined

executed = generate_combined_pivots(pairs, "executed")
approved = generate_combined_pivots(pairs, "approved")

# COMMAND ----------

# Helper functions to update the EXCEL sheets
#todo: move to utils for reusability when we have more than one country

def spark_to_pandas_with_reorder(ws, raw_data):
    # make sure that the data expendture column specific order so that the formula will work
    raw_data = raw_data.toPandas()
    ws = source_wb['Data_Expenditures']
    column_names = [cell.value for cell in ws[1] if cell.value is not None]  # Row 1 is typically the header

    remaining = [col for col in raw_data.columns if col not in column_names]
    new_order = column_names + remaining
    for col in column_names:
        if col not in raw_data.columns:
            raw_data[col] = None
    raw_data = raw_data[new_order]
    return raw_data

def copy_font(cell, blue_text_format=False):
    if not cell.font:
        return {}
    # Copy font formatting
    if cell.font.bold:
        bold = True
    else:
        bold = False
    
    if cell.font.italic:
        italic = True
    else:
        italic = False

    num_format = cell.number_format

    font_size = cell.font.size if cell.font.size else 10  # Default font size if none specified
    font_name = cell.font.name if cell.font.name else 'Arial'  # Default to Arial if no font specified

    # source worksheet does not have color information. use heuristic to set the color. 
    if cell.row == 1:
        font_color = "#FFFFFF"
        bg_color = "#4d93d9"
    else:
        font_color = 'blue' if blue_text_format else None
        bg_color = "#FFFFFF"
    
    if cell.column < 3:
        align = 'left'
    else:
        align = 'right'

    return {
        'bold': bold,
        'italic': italic,
        'font_name': font_name,
        'font_size': font_size,
        'align': align,
        'valign': 'vcenter',
        'num_format': num_format,
        'font_color':font_color,
        "bg_color": bg_color
    }

def set_width(target_ws,max_col_index):
    for i in range(max_col_index):
        if i == 0:
            width = 30
        elif i == 1:
            width = 50
        else:
            width = 12
        target_ws.set_column(i, i, width)  # xlsxwriter uses 0-based index

def get_col_name(source_ws,col_inedex, current_year):
    # some of the years are evaluated in formula: e.g. =U1+1.
    # load_workbook with data_only=True was too expensive. 
    # we need to evaluate the formula to get the year
    col_name = source_ws.cell(row=1, column=col_inedex+1).value
    if str(col_name).startswith("="):
        col_name = current_year + 1
    return col_name


def update_excel_with_new_values(target_ws, source_ws, df):    
    max_row = source_ws.max_row
    max_col = source_ws.max_column 

    df = df.toPandas()
    # Copy cell values and styles
    col_name = None
    for row_index in range(0, max_row):
        code = source_ws.cell(row=row_index+1, column=1).value
        for col_inedx in range(0, max_col-1):
            col_name = get_col_name(source_ws,col_inedx, col_name)
            source_cell = source_ws.cell(row=row_index+1, column=col_inedx+1)

            default_cell_format = target_wb.add_format(copy_font(source_cell))

            if str(col_name) not in years or code not in df.Code.values:
                if source_cell.data_type == 'f':
                    blue_cell_format = target_wb.add_format(copy_font(source_cell, blue_text_format=True))
                    target_ws.write_formula(row_index, col_inedx, str(source_cell.value), blue_cell_format)  # Write the formula to Excel
                else:
                    target_ws.write(row_index, col_inedx, source_cell.value,default_cell_format)
            else:                
                boost_value = df[str(col_name)][df['Code'] == code].values[0]
                target_ws.write(row_index, col_inedx, boost_value,default_cell_format)
        set_width(target_ws, max_col)

# COMMAND ----------

# Load an existing workbook
source_wb = load_workbook(SOURCE_FILE_PATH,  data_only=False, read_only=True,)  # Important: data_only=False to get formulas
required_sheets = ["Executed", "Approved"]
for sheet in required_sheets:
    if sheet not in source_wb.sheetnames:
        raise KeyError(f"Required sheet '{sheet}' is missing in the source workbook.")

with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=True) as tmp:

    temp_path = tmp.name

    # Now write the updated DataFrame to a new Excel file
    with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
        raw_data = spark_to_pandas_with_reorder(source_wb["Executed"], raw_data)[:10]
        raw_data.to_excel(writer, sheet_name='Data_Expenditures', index=False)

        revenue = pd.DataFrame() # revenue data sheet is currently empty but needed for the formula reference
        revenue.to_excel(writer, sheet_name='Data_Revenues', index=False)

        # Access the xlsxwriter workbook and sheet objects
        target_wb  = writer.book

        # add name manager to the target workbook
        for name, defn in source_wb.defined_names.items():
            if name in ["level", "econ0", "econ_func"]:
                continue
            target_wb.define_name(name, f"={defn.attr_text}")


        # Create an 'Executed' sheet
        target_ws = target_wb.add_worksheet('Executed')  
        source_ws = source_wb['Executed'] 
        update_excel_with_new_values(target_ws, source_ws, executed)

        # Create an 'Approved' sheet
        target_ws = target_wb.add_worksheet('Approved')  
        source_ws = source_wb['Approved'] 
        update_excel_with_new_values(target_ws, source_ws, approved)

    source_wb.close()
    shutil.copy(temp_path, OUTPUT_FILE_PATH)
