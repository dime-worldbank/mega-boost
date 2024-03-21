# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %pip install unidecode

# COMMAND ----------

import re
import pandas as pd
from unidecode import unidecode

COUNTRY = 'Colombia'
microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)

def normalize(cell):
    # downcase, remove white space, newline, non-word, accent
    return re.sub(r'[\s\W]', '', unidecode(cell)).lower()

# COMMAND ----------

HEADER = ["EntidadDetalle", "ApropiacionDefinitiva", "Compromiso", "Obligacion", "Pago"]
central_files = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/central/execution/*.xlsx')
for f in central_files:
    num_sheets = len(pd.ExcelFile(f).sheet_names)
    sheet_name = 0 if num_sheets == 1 else 'Cuadro No. 7'
    df = pd.read_excel(f, sheet_name=sheet_name, header=None)

    # Search year, starting row & column index
    row_index = None
    col_index = None
    year = None
    year_pattern = r'Acumulada a diciembre de (\d{4})'
    for i, row in df.iterrows():
        for j, value in enumerate(row):
            if type(value) == str:
                year_matched = re.match(year_pattern, value, re.IGNORECASE)
                if year_matched:
                    year = int(year_matched.group(1))
                elif value == 'TOTAL':
                    row_index = i
                    col_index = j
                    break
        if row_index is not None:
            break
    
    assert year is not None, f'Failed to parse year out of central execution file {f}'

    skiprows = row_index-1
    usecols = range(col_index, col_index+5)
    df = pd.read_excel(f, sheet_name=sheet_name, skiprows=skiprows, usecols=usecols, names=HEADER)
    df['year'] = year
    df.index.name = 'raw_row_id'
    print(year, row_index, col_index, df.shape)

    assert df.shape[0] > 4000, f'Expected more than 4000 rows of line items but got {df.shape[0]} from {f}'
    assert df.shape[1] == 6, f'Expected 6 columns but got {df.shape[1]}: {df.columns} from {f}'

    outfile = f'{microdata_csv_dir}/central_execution_{year}.csv'
    df.to_csv(outfile)

# COMMAND ----------

earlier_filenames = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/subnational/gastos/*_FUT_*.xlsx')
recent_filenames = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/subnational/gastos/*_EJECUCION_*.xlsx')

EJECUCION_REQUIRED_COLS = [
    'codigofut', # geo code
    'concepto_cod', 'nombreconcepto', # budget code
    'seccionpresupuestal', 'nombreseccion', # budget section
    'vigenciagasto', # validity? needed for partitioning/grouping before ordering by budget code
    'compromisos', 'obligaciones', 'pagos' # numbers
]
for filename in earlier_filenames + recent_filenames:
    filename_stem = Path(filename).stem
    outfile = f'{microdata_csv_dir}/subnational_gastos_{filename_stem}.csv'
    
    if dbfs_file_exists(outfile.replace('/dbfs', '')):
        continue

    df = pd.read_excel(filename, sheet_name=0)
    df.columns = df.columns.map(normalize)

    print(filename_stem, df.shape)
    min_num_rows_subnat_earlier = 200000
    min_num_rows_subnat_recent = 650000
    if 'GastosFuncionamiento' in filename:
        assert len(df.columns) == 17
        assert df.shape[0] >= min_num_rows_subnat_earlier, f'Expect to find at least {min_num_rows_subnat_earlier}, but found {df.shape[0]} row'
    elif 'GastosInversion' in filename:
        assert len(df.columns) == 15
        assert df.shape[0] >= min_num_rows_subnat_earlier, f'Expect to find at least {min_num_rows_subnat_earlier}, but found {df.shape[0]} row'
    elif 'EJECUCION' in filename:
        df = df.rename(columns={'nombreseccionpresupuestal': 'nombreseccion'})
        for col_name in EJECUCION_REQUIRED_COLS:
            assert col_name in df.columns, f'Expect column named {col_name} to exist in {df.columns} in {filename_stem}'
        assert df.shape[0] >= min_num_rows_subnat_recent, f'Expect to find at least {min_num_rows_subnat_recent}, but found {df.shape[0]} row'
        df = df[EJECUCION_REQUIRED_COLS]
        # trim leading and trailing whitespaces from text columns
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

    df.index.name = 'raw_row_id'
    df['year'] = int(filename_stem.split('_')[0])
    df.to_csv(outfile)

# COMMAND ----------

# Copy all auxiliary files into csv folder for DLT consumption
dbutils.fs.cp(f'{RAW_INPUT_DIR}/{COUNTRY}/auxiliary'.replace('/dbfs', ''),       
              microdata_csv_dir.replace('/dbfs', ''), recurse=True)

# COMMAND ----------

dbutils.fs.ls(microdata_csv_dir.replace('/dbfs', ''))
