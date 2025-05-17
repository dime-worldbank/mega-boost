# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %pip install xlsxwriter

# COMMAND ----------

import tempfile
import shutil

from pyspark.sql.functions import col, sum as _sum, lit
from pyspark.sql import functions as F

import pandas as pd
from openpyxl import load_workbook

import logging
logger = logging.getLogger()  # Get the root logger
logging.getLogger("py4j").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

APPLY_BLUE_FONT_IF_MISSING = False

OUTPUT_DIR = f"{TOP_DIR}/Workspace/output_excel"
AUXI_DIR = f"{TOP_DIR}/Documents/input/Auxiliary"
INPUT_AUXI_DIR = f"{TOP_DIR}/Documents/input/Auxiliary"

OUTPUT_FILE_PATH = f"{OUTPUT_DIR}/Albania_BOOST.xlsx"
SOURCE_FILE_PATH = f"{INPUT_DIR}/Albania BOOST.xlsx"
TARGET_TABLE = 'prd_mega.boost.alb_publish'



# COMMAND ----------


# Load and filter
raw_data = (
    spark.table(f"{TARGET_TABLE}")
    .withColumn("year", col("year").cast("string"))
    .cache()
)

required_columns = [
    "boost_admin0",
    "boost_admin1",
    "boost_admin2",
    "boost_econ",
    "boost_econ_sub",
    "boost_func",
    "boost_func_sub",
    "boost_is_foreign",
    "boost_approved",
    "boost_executed",
    "boost_year",
]

for col_name in required_columns:
    if col_name not in raw_data.columns:
        raise KeyError(f"Required column '{col_name}' is missing in the DataFrame.")

tag_code_mapping = pd.read_csv(f"{AUXI_DIR}/tag_label_mapping.csv") 
years = [str(year) for year in sorted(raw_data.select("year").distinct().rdd.flatMap(lambda x: x).collect())]

def create_pivot(df, parent, child, agg_col ):
    filtered_mapping = tag_code_mapping[tag_code_mapping['subnational'].isna()]

    # Step 1: Get detailed level e.g. (econ + econ_sub + year)
    detailed = (
        df.groupBy(parent, child, "boost_year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumnRenamed(parent, "parent")
        .withColumnRenamed(child, "child")
    )

    # Step 2: Get subtotals at parent level e.g (econ + year), econ_sub = 'Subtotal'
    subtotals = (
        df.groupBy(parent, "boost_year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumn("child", F.lit("Subtotal"))  # Ensure same schema
        .withColumnRenamed(parent, "parent")
    )

    # Step 3: Union both
    combined = detailed.unionByName(subtotals).filter(F.col("boost_year").isNotNull())

    # Step 4: Pivot to wide format
    pivoted = (
        combined.groupBy("parent", "child")
        .pivot("boost_year")
        .agg(F.sum(agg_col))
        .fillna(0)  # Replace NaNs with 0
    )

    # Step 5: Join with filtered_mapping
    filtered_mapping_spark = spark.createDataFrame(filtered_mapping)
    result = (
        pivoted.join(filtered_mapping_spark, on=["parent", "child"])
        .drop("Categories", "parent", "child", "parent_type", "child_type", "subnational")
    )

    total_matches = result.select("Code").distinct().rdd.flatMap(lambda x: x).collect()
    logging.info(f"Matched {agg_col} entries for {parent}, {child} : {len(total_matches)} ")

    return result


# COMMAND ----------

def create_pivot_total(df, agg_col):
    # Step 1: Calculate total expenditure by year
    total = (
        df.groupBy("boost_year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumn("Code", F.lit("EXP_ECON_TOT_EXP_EXE"))
    )

    # Step 2: Calculate foreign expenditure by year
    foreign_total = (
        df.filter(F.col("boost_is_foreign") == True)
        .groupBy("boost_year")
        .agg(F.sum(agg_col).alias(agg_col))
        .withColumn("Code", F.lit("EXP_ECON_TOT_EXP_FOR_EXE"))
    )

    # Step 3: Combine total and foreign expenditure
    combined = total.unionByName(foreign_total).filter(F.col("boost_year").isNotNull())

    # Step 4: Pivot to wide format
    pivoted = (
        combined.groupBy("Code")
        .pivot("boost_year")
        .agg(F.first(agg_col))
        .fillna(0)  
    )
    return pivoted

# COMMAND ----------

pairs = [('boost_econ', 'boost_econ_sub'), ('boost_func', 'boost_econ_sub'), ('boost_func', 'boost_econ'), ('boost_func', 'boost_func_sub'), ('boost_func_sub', 'boost_econ'), ('boost_func_sub', 'boost_econ_sub')]

def generate_combined_pivots(pairs, agg_col):
    # Initialize an empty DataFrame for combining results
    combined = None

    for parent, child in pairs:
        # Create pivot for central and regional levels
        pivoted = create_pivot(raw_data, parent, child,agg_col)
        
        # Combine the results
        if combined is None:
            combined = pivoted
        else:
            combined = combined.unionByName(pivoted)

    # Add totals to the combined DataFrame
    totals = create_pivot_total(raw_data, agg_col)
    combined = combined.unionByName(totals)
    return combined

executed = generate_combined_pivots(pairs, "boost_executed")
approved = generate_combined_pivots(pairs, "boost_approved")

# COMMAND ----------

# Helper functions to update the EXCEL sheets
#todo: move to utils for reusability when we have more than one country
EXCEL_COL_LIST =  [
    "Code", "Categories", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", 
    "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026", 
    "2027", "2028", "2029", "2030", "AVG", "Country", "Country", "Fomrulas", "Coverage", 
    "Fiscal year", "Region", "Income", "Basis", "Source", "Rigidity", "empty"
]

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
    bold = True if cell.font.bold else False
    italic = True if cell.font.italic else False
    num_format = cell.number_format
    font_size = cell.font.size if cell.font.size else 10  # Default font size if none specified
    font_name = cell.font.name if cell.font.name else 'Arial'  # Default to Arial if no font specified

    # source worksheet does not have other styling information. use heuristic to set the color/alignment. 
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

def get_col_name(col_inedex):
    # Some year columns in the original file use formulas (e.g., =U1+1),
    # which makes evaluating the actual column names dynamically too costly.
    # As a workaround, we hardcoded the column names from the original file
    # and iterate through that list instead of computing them.
    col_name = EXCEL_COL_LIST[col_inedex]
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
            col_name = get_col_name(col_inedx)
            source_cell = source_ws.cell(row=row_index+1, column=col_inedx+1)

            default_cell_format = target_wb.add_format(copy_font(source_cell))

            if str(col_name) not in years or code not in df.Code.values:
                if source_cell.data_type == 'f':
                    cell_format = target_wb.add_format(copy_font(source_cell, blue_text_format=APPLY_BLUE_FONT_IF_MISSING))
                    formula = getattr(source_cell.value, "text", source_cell.value)
                    target_ws.write_formula(row_index, col_inedx, formula, cell_format)  
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

# pandas.to_excel() cannot write directly to DBFS paths like '/dbfs/...'.
# Workaround: write to a temporary local file, then copy it to the target DBFS location.
with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=True) as tmp:
    temp_path = tmp.name

    # openpyxl was too resource-intensive for writing Excel files with formatting and styles for our use case.
    # Given our cluster limitations, xlsxwriter was the only viable solution for efficient output.
    with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
        raw_data = spark_to_pandas_with_reorder(source_wb["Executed"], raw_data)
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

# COMMAND ----------

coverage_start = raw_data.year.min()
coverage_end = raw_data.year.max()
coverage_country = "Albania"
tagging_sql = f"""
ALTER TABLE {TARGET_TABLE} SET TAGS (
    'name' = 'Albania BOOST {coverage_start}-{coverage_end}',
    'comment' = 'The Ministry of Finance of Albania together with the World Bank developed and published a BOOST platform obtained from the National Treasury System in order to facilitate access to the detailed public finance data for comprehensive budget analysis. In this context, the Albania BOOST Public Finance Portal aims to strengthen the disclosure and demand for availability of public finance information at all level of government in the country from 2010 onward.Note that 2020 execution only covers 6 months.',
    'subject' = 'Finance',
    'classification' = 'Official Use Only',
    'category' = 'Public Sector',
    'subcategory' = 'Financial Management',
    'frequency' = 'Annually',
    'collections' = 'Financial Management (FM), BOOST - Public Expenditure Database',
    'source' = 'BOOST',
    'domain' = 'Budget',
    'subdomain' = 'Budget & Cost Accounting',
    'excel_link' = '/Volumes/prd_mega/sboost4/vboost4/Workspace/output_excel/Albania_BOOST.xlsx',
    'destinations' = 'dataexplorer, ddh',
    'exception' = '7. Member Countries/Third Party Confidence',
    'license' = 'Creative Commons Attribution-Non Commercial 4.0',
    'topics' = 'Economic Growth, Macroeconomic and Structural Policies, Public Sector Management',
    'coverage_year_start' = '{coverage_start}',
    'coverage_year_end' = '{coverage_end}',
    'coverage_countries' = '{coverage_country}',
    'team_lead' = 'mmastruzzi@worldbank.org',
    'collaborators' = 'icapita@worldbank.org, agirongordillo@worldbank.org, sbhupatiraju@worldbank.org, ysuzuki2@worldbank.org, elysenko@worldbank.org, wlu4@worldbank.org'
);
"""

spark.sql(tagging_sql)
