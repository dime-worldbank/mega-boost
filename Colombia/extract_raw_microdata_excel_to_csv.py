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

df

# COMMAND ----------

dbutils.fs.ls(f'{RAW_INPUT_DIR}/{COUNTRY}/subnational/gastos/'.replace('/dbfs', ''))

# COMMAND ----------

dbutils.fs.ls(f'{microdata_csv_dir}'.replace('/dbfs', ''))
