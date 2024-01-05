# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

from tqdm import tqdm
import unicodedata
import pandas as pd
import openpyxl
import csv

COUNTRY = 'Paraguay'
microdata_csv_dir = prepare_microdata_csv_dir(COUNTRY)
filename = input_excel_filename(COUNTRY)

# TODO: Figure out the difference between cen and Municipalidades
disaggregated_data_sheets = ['cen']
for sheet in tqdm(disaggregated_data_sheets):
    csv_file_path = f'{microdata_csv_dir}/{sheet}.csv'
    # Open the Excel file with openpyxl
    wb = openpyxl.load_workbook(filename, read_only=True)
    sheet_data = wb[sheet]

    # Write the data to a CSV file
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header row
        full_header_row = [cell.value for cell in next(sheet_data.iter_rows(max_row=1))]
        mask = [is_named_column(cell) for cell in next(sheet_data.iter_rows(max_row=1))]
        header_row = [x for x, include in zip(full_header_row, mask) if include]
        csv_writer.writerow(header_row)
        
        # Write data rows
        for row in sheet_data.iter_rows(min_row=2):
            normalized_row = [normalize_cell(cell.value) for i, cell in enumerate(row) if mask[i]]
            csv_writer.writerow(normalized_row)
