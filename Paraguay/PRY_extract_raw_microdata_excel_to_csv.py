# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import os
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
COUNTRY = 'Paraguay'
if IS_DATABRICKS:
    # TOP_DIR, RAW_INPUT_DIR, prepare_raw_microdata_csv_dir, normalize_cell and is_named_column come from %run ../utils
    raw_microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)
    AUXILIARY_DIR = f'{TOP_DIR}/Documents/input/Auxiliary/{COUNTRY}'  # mapping files
else:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # utils.py sits at the repo root
    from utils import normalize_cell, is_named_column
    RAW_INPUT_DIR = os.environ.get('RAW_INPUT_DIR') or input("Enter the raw input directory (containing Paraguay/): ").strip()
    AUXILIARY_DIR = os.environ.get('AUXILIARY_DIR') or input("Enter the auxiliary directory (containing the PRY_*_mapping.csv files): ").strip()
    raw_microdata_csv_dir = os.environ.get('OUTPUT_DIR') or input("Enter the output directory: ").strip()
    Path(raw_microdata_csv_dir).mkdir(parents=True, exist_ok=True)

# The two files the Ministry of Economy and Finance (VEP) sends, each a single "Database" sheet.
CENTRAL_FILE_PATTERN = 'Gastos Administraci*n Central y Descentralizadas*.xlsx'
MUNICIPAL_FILE_PATTERN = 'Gastos*Municipios*.xlsx'

# Ministry header -> column name used by the BOOST CCI workbook (sheets `cen` and `Municipalidades`)
CENTRAL_COLUMNS = {
    'Año': 'YEAR',
    'Nivel': 'ADMIN1',
    'Entidad': 'ADMIN2',
    'Unidad Responsable': 'ADMIN3',
    'Tipo Presupuesto': 'BUDGET_TYPE',
    'Programa': 'PROGRAM1',
    'Sub-programa': 'PROGRAM2',
    'Proyecto': 'PROJECT1',
    'Finalidad': 'FUNCTION1',
    'Función': 'FUNCTION2',
    'Sub-función': 'FUNCTION3',
    'Grupo Económico': 'ECON1',
    'Sub-grupo económico': 'ECON2',
    'Económico': 'ECON3',
    'Grupo Objeto del Gasto': 'ECON4',
    'Sub-grupo Objeto del Gasto': 'ECON5',
    'Objeto del Gasto': 'ECON6',
    'Fuente de Financiamiento': 'FIN_SOURCE1',
    'Origen de Financiamiento': 'FIN_SOURCE2',
    'Departamento': 'GEO1',
    'Transferencia Consolidable': 'TRANSFER',
    'Presupuesto Inicial': 'APPROVED',
    'Presupuesto Vigente': 'MODIFIED',
    'Ejecutado': 'COMMITTED',
    'Pagado': 'PAID',
}
MUNICIPAL_COLUMNS = {
    'Año': 'YEAR',
    'Nivel': 'ADMIN1',
    'Entidad': 'ADMIN2',
    'Grupo Objeto del Gasto': 'ECON4',
    'Sub-Grupo Objeto del Gasto': 'ECON5',
    'Presupuesto Vigente (millones de guaraníes)': 'MODIFIED',
    'Ejecutado (millones de guaraníes)': 'COMMITTED', # Ejecutado corresponds to the executed budget for municipalities
    'Pagado (millones de guaraníes)': 'PAID',
}
FIRST_YEAR = 2006  # the CCI workbook covers 2006 onward; the ministry file also holds 2003-2005

# The derived columns of the BOOST workbook's central sheet (Paraguay BOOST.xlsx `Central`, CCI `cen`). The User
# Manual (User Manual - PAR.docx) describes the first three.
# TRANSFERS TO MUNICIPALITIES: "Objeto del Gasto" (ECON6) codes
TRANSFER_ECON6_CODES = ['833', '836', '893', '896']
# FOREIGN FUNDED EXPENSES: "Origen de Financiamiento" (FIN_SOURCE2) codes listed in the manual. Review when a new
# foreign source appears.
FOREIGN_FIN_SOURCE2_CODES = [
    '302', '303', '359',
    '401', '402', '403', '404', '405', '406', '409',
    '503', '505', '508', '509', '510', '513', '514', '519', '599',
    '602', '604', '606', '607', '609',
    '653', '654', '655', '659',
    '815', '816',
]
# ROAD TRANSPORTATION: "Proyecto" (PROJECT1) names containing ruta, camino, corredor or vial as whole words, outside
# sub-function 431 - TRANSPORTE TERRESTRE.
ROAD_PROJECT_KEYWORDS = r'\b(RUTAS?|CAMINOS?|CORREDOR(ES)?|CORRIDORS?|VIAL(ES|IDAD)?)\b'
ROAD_FUNCTION3_CODE = '431'
# Reviewed project names: road = y flags a name the key words miss, road = n removes a false positive. Each run
# prints the matched names that are not reviewed yet; add them to the file with a decision.
PROJECT_MAPPING_FILE = f'{AUXILIARY_DIR}/PRY_project_mapping.csv'
# HOSPITAL: sub-programmes (PROGRAM2) whose name contains hospital, hospitales or hospitalario/a as a whole word.
HOSPITAL_PROGRAM2_KEYWORDS = r'\bHOSPITAL(ES|ARIOS?|ARIAS?)?\b'

# COMMAND ----------

# Department of each municipality, by entity code (30.001 - ASUNCION -> 30.001), since the ministry file has no
# department column. A new municipality gets a new code and must be added to the file.
ADMIN_MAPPING_FILE = f'{AUXILIARY_DIR}/PRY_admin_mapping.csv'

# COMMAND ----------

# TODO: generalize when the year coverage is updated. This reads one file per tier with all years in a single
# "Database" sheet; later deliveries may come as yearly files, extra sheets or renamed columns.
def read_database(pattern, columns):
    """Read the ministry's single-sheet file, keep the named columns, rename them to the BOOST names, normalize cells."""
    files = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/{pattern}')
    assert len(files) == 1, f'expect one file matching {pattern} in {RAW_INPUT_DIR}/{COUNTRY}, found {files}'
    print(f'reading {files[0]}')
    df = pd.read_excel(files[0], sheet_name='Database', header=0)

    # Handle unnamed or null named columns
    header = [col_name for col_name in df.columns if is_named_column(col_name)]
    df = df[header]
    df.columns = [col.strip() for col in header]

    missing = set(columns) - set(df.columns)
    assert not missing, f'columns missing from the ministry file: {sorted(missing)}'
    df = df[list(columns)].rename(columns=columns)

    # Normalize cells (accents stripped; non-text cells pass through unchanged)
    df = df.apply(lambda column: column.map(normalize_cell))
    return df


def code(series):
    """The numeric code that starts every label, e.g. '833' from '833 - TRANSFERENCIAS A MUNICIPALIDADES'."""
    return series.str.strip().str[:3]


def flag(condition, value):
    return np.where(condition, value, None)

# COMMAND ----------

# Central and decentralised administration -> cen.csv
df = read_database(CENTRAL_FILE_PATTERN, CENTRAL_COLUMNS)
df = df[df.YEAR >= FIRST_YEAR]
for col in ['APPROVED', 'MODIFIED', 'COMMITTED', 'PAID']:
    df[col] = df[col].round().astype('Int64')  # whole guaraníes; a missing cell stays empty instead of turning the column into floats

df['TRANS'] = flag(code(df.ECON6).isin(TRANSFER_ECON6_CODES), 'TRANS')
df['Foreign'] = flag(code(df.FIN_SOURCE2).isin(FOREIGN_FIN_SOURCE2_CODES), 'Foreign')
project_mapping = pd.read_csv(PROJECT_MAPPING_FILE, dtype=str)
project_mapping['project1'] = project_mapping.project1.map(normalize_cell).str.strip().str.upper()
assert project_mapping.project1.is_unique, f'duplicate project names in {PROJECT_MAPPING_FILE}'
reviewed_road = project_mapping.set_index('project1').road.str.strip().str.lower().eq('y')
project = df.PROJECT1.str.strip().str.upper()
keyword_hit = project.str.contains(ROAD_PROJECT_KEYWORDS, regex=True, na=False)
decision = project.map(reviewed_road)  # True = y, False = n, NaN = not reviewed yet
outside_431 = ~df.FUNCTION3.str.startswith(ROAD_FUNCTION3_CODE, na=False)
df['Road'] = flag(np.where(decision.notna(), decision, keyword_hit).astype(bool) & outside_431, 'y')
unreviewed = sorted(set(project[keyword_hit & decision.isna() & outside_431]))
print(f'{len(unreviewed)} road-matching project names are not in {PROJECT_MAPPING_FILE}; '
      'they are flagged by the key words, review them and add them with road = y or n:')
print('\n'.join(f'   {name}' for name in unreviewed))
df['hospital'] = flag(df.PROGRAM2.str.contains(HOSPITAL_PROGRAM2_KEYWORDS, case=False, regex=True, na=False), 1)

df.to_csv(f'{raw_microdata_csv_dir}/cen.csv', index=False, encoding='utf-8')
print(f'cen.csv: {len(df):,} rows')
print(df.groupby('YEAR').agg(rows=('YEAR', 'size'), approved=('APPROVED', 'sum'), committed=('COMMITTED', 'sum'),
                             transfers=('TRANS', 'count'), foreign=('Foreign', 'count'),
                             roads=('Road', 'count'), hospitals=('hospital', 'count')))

# COMMAND ----------

# Municipalities -> Municipalidades.csv
df = read_database(MUNICIPAL_FILE_PATTERN, MUNICIPAL_COLUMNS)
df = df[df.YEAR >= FIRST_YEAR]

# The ministry publishes municipal amounts in millions of guaraníes; the CCI workbook holds guaraníes.
for col in ['MODIFIED', 'COMMITTED', 'PAID']:
    df[col] = (df[col] * 1_000_000).round().astype('Int64')
df['approved'] = None  # not published for municipalities
df['ECON6'] = None  # the ministry file stops at "Sub-Grupo Objeto del Gasto"

# Department of each municipality, keyed on the entity code (30.001 - ASUNCION -> 30.001).
admin_mapping = pd.read_csv(ADMIN_MAPPING_FILE, dtype=str)
assert admin_mapping.admin2_code.is_unique, f'duplicate municipality codes in {ADMIN_MAPPING_FILE}'
municipality_department = admin_mapping.set_index('admin2_code').department
admin2_code = df.ADMIN2.str.strip().str[:6]
unmapped = sorted(set(admin2_code) - set(municipality_department.index))
assert not unmapped, f'municipalities missing from {ADMIN_MAPPING_FILE}: {unmapped}'
df['geo'] = admin2_code.map(municipality_department).map(normalize_cell)

df = df[['YEAR', 'ADMIN1', 'ADMIN2', 'ECON4', 'ECON5', 'ECON6', 'geo', 'approved', 'MODIFIED', 'COMMITTED', 'PAID']]
df.to_csv(f'{raw_microdata_csv_dir}/Municipalidades.csv', index=False, encoding='utf-8')
print(f'Municipalidades.csv: {len(df):,} rows')
print(df.groupby('YEAR').agg(rows=('YEAR', 'size'), modified=('MODIFIED', 'sum'), paid=('PAID', 'sum'),
                             municipalities=('ADMIN2', 'nunique'), departments=('geo', 'nunique')))
