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
    # RAW_INPUT_DIR, prepare_raw_microdata_csv_dir, normalize_cell and is_named_column come from %run ../utils
    raw_microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)
else:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # utils.py sits at the repo root
    from utils import normalize_cell, is_named_column
    RAW_INPUT_DIR = os.environ.get('RAW_INPUT_DIR') or input("Enter the raw input directory (containing Paraguay/): ").strip()
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
    'Ejecutado (millones de guaraníes)': 'COMMITTED',
    'Pagado (millones de guaraníes)': 'PAID',
}
FIRST_YEAR = 2006  # the CCI workbook covers 2006 onward; the ministry file also holds 2003-2005

# The derived columns of the BOOST workbook's central sheet (Paraguay BOOST.xlsx `Central`, CCI `cen`). The User
# Manual (User Manual - PAR.docx) describes the first three.
# TRANSFERS TO MUNICIPALITIES: "Objeto del Gasto" (ECON6) codes
TRANSFER_ECON6_CODES = ['833', '836', '893', '896']
# FOREIGN FUNDED EXPENSES: "Origen de Financiamiento" (FIN_SOURCE2) codes listed in the manual; Paraguay BOOST.xlsx
# flags exactly these (including 815/816, LEY 5074/2013 contributions). Review when a new foreign source appears.
FOREIGN_FIN_SOURCE2_CODES = [
    '302', '303', '359',
    '401', '402', '403', '404', '405', '406', '409',
    '503', '505', '508', '509', '510', '513', '514', '519', '599',
    '602', '604', '606', '607', '609',
    '653', '654', '655', '659',
    '815', '816',
]
# ROAD TRANSPORTATION: the manual says to filter "Proyecto" (PROJECT1) by the key words ruta, camino, corredor
# outside sub-function 431 - TRANSPORTE TERRESTRE. "vial" is added because it carries a quarter of road spending
# (obras viales, red vial, accesos viales). Whole words only, so "piedra bruta" and "fluviales" do not match.
ROAD_PROJECT_KEYWORDS = r'\b(RUTAS?|CAMINOS?|CORREDOR(ES)?|CORRIDORS?|VIAL(ES|IDAD)?)\b'
ROAD_FUNCTION3_CODE = '431'
# Reviewed project names, kept on the volume next to the ministry files: road = y flags a name the key words miss,
# road = n removes a false positive. Each run prints the matched names that are not reviewed yet; add them to the
# file with a decision.
PROJECT_MAPPING_FILE = f'{RAW_INPUT_DIR}/{COUNTRY}/PRY_project_mapping.csv'
# hospital is not in the manual: the workbook flags the sub-programmes (PROGRAM2) whose name contains HOSPITAL
HOSPITAL_PROGRAM2_KEYWORD = 'HOSPITAL'

# COMMAND ----------

# Department of each municipality, by entity code (30.001 - ASUNCION -> 30.001): the ministry file has no department
# column. Built from the consistent years of Paraguay BOOST.xlsx (2012 onward), Chaco set to the official codes
# 16 Boqueron / 17 Alto Paraguay. A municipality created later gets a new code and must be added here.
DEPARTMENT_MUNICIPALITIES = {
    # Asuncion
    '00 - ASUNCION': [
        '30.001'
    ],
    # Concepcion, Belen, Horqueta, Loreto, San Lazaro, Yby Yau, San Carlos Del Apa, Azotey, Sargento Jose Felix
    # Lopez, Paso Barreto, San Alfredo, Arroyito, Paso Horqueta, Itacua
    '01 - CONCEPCION': [
        '30.002', '30.003', '30.004', '30.005', '30.006', '30.007', '30.237', '30.238', '30.241', '30.248',
        '30.249', '30.254', '30.257', '30.263'
    ],
    # San Pedro Del Ycuamandiyu, Antequera, Chore, Gral. Elizardo Aquino, Itacurubi Del Rosario, Lima, Nueva
    # Germania, San Estanislao, Yrybucua, San Pablo, Tacuati, Union, 25 De Diciembre, Villa Del Rosario, Gral.
    # Resquin, Yataity Del Norte, Guajayvi, Capiibary, Santa Rosa Del Aguaray, Liberacion, San Vicente Pancholo,
    # San Jose Del Rosario
    '02 - SAN PEDRO': [
        '30.008', '30.009', '30.010', '30.011', '30.012', '30.013', '30.014', '30.015', '30.016', '30.017',
        '30.018', '30.019', '30.020', '30.021', '30.022', '30.023', '30.024', '30.025', '30.026', '30.239',
        '30.253', '30.260'
    ],
    # Caacupe, Altos, Arroyos Y Esteros, Atyra, Caraguatay, Emboscada, Eusebio Ayala, Isla Pucu, Itacurubi De Las
    # Cordilleras, Juan De Mena, Loma Grande, Mbocayaty Del Yhaguy, Nueva Colombia, Piribebuy, Primero De Marzo,
    # San Bernardino, Santa Elena, Tobati, Valenzuela, San Jose Obrero
    '03 - CORDILLERA': [
        '30.027', '30.028', '30.029', '30.030', '30.031', '30.032', '30.033', '30.034', '30.035', '30.036',
        '30.037', '30.038', '30.039', '30.040', '30.041', '30.042', '30.043', '30.044', '30.045', '30.046'
    ],
    # Villarrica, Borja, Mauricio Jose Troche, Coronel Martinez, Felix Perez Cardozo, Gral. Eugenio A. Garay,
    # Independencia, Itape, Iturbe, Jose Fassardi, Mbocayaty Del Guaira, Natalicio Talavera, Numi, San Salvador,
    # Yataity Del Guaira, Dr. Bottrell, Paso Yobai, Tebicuary
    '04 - GUAIRA': [
        '30.047', '30.048', '30.049', '30.050', '30.051', '30.052', '30.053', '30.054', '30.055', '30.056',
        '30.057', '30.058', '30.059', '30.060', '30.061', '30.062', '30.063', '30.234'
    ],
    # Coronel Oviedo, Caaguazu, Carayao, Dr. Cecilio Baez, Santa Rosa Del Mbutuy, Juan Manuel Frutos,
    # Repatriacion, Nueva Londres, San Joaquin, San Jose De Los Arroyos, Yhu, J. Eulogio Estigarribia, R.I. 3
    # Corrales, Raul Arsenio Oviedo, Jose Domingo Ocampos, Mcal. Francisco Solano Lopez, La Pastora, 3 De Febrero,
    # Simon Bolivar, Vaqueria, Tembiapora, Nueva Toledo
    '05 - CAAGUAZU': [
        '30.064', '30.065', '30.066', '30.067', '30.068', '30.069', '30.070', '30.071', '30.072', '30.073',
        '30.074', '30.075', '30.076', '30.077', '30.078', '30.079', '30.080', '30.081', '30.082', '30.083',
        '30.233', '30.243'
    ],
    # Caazapa, Avai, Buena Vista, Moises Bertoni, Gral. Higinio Morinigo, Maciel, San Juan Nepomuceno, Tavai,
    # Fulgencio Yegros, Yuty, 3 De Mayo
    '06 - CAAZAPA': [
        '30.084', '30.085', '30.086', '30.087', '30.088', '30.089', '30.090', '30.091', '30.092', '30.093',
        '30.245'
    ],
    # Encarnacion, Bella Vista - Itapua, Cambyreta, Capitan Meza, Capitan Miranda, Nueva Alborada, Carmen Del
    # Parana, Coronel Bogado, Carlos Antonio Lopez, Natalio, Fram, General Artigas, General Delgado, Hohenau,
    # Jesus, Jose Leandro Oviedo, Obligado, Mayor Otano, San Cosme Y Damian, San Pedro Del Parana, San Rafael Del
    # Parana, Trinidad, Edelira, Tomas Romero Pereira, Alto Vera, La Paz, Yatytay, San Juan Del Parana, Pirapo,
    # Itapua Poty
    '07 - ITAPUA': [
        '30.094', '30.095', '30.096', '30.097', '30.098', '30.099', '30.100', '30.101', '30.102', '30.103',
        '30.104', '30.105', '30.106', '30.107', '30.108', '30.109', '30.110', '30.111', '30.112', '30.113',
        '30.114', '30.115', '30.116', '30.117', '30.118', '30.119', '30.120', '30.121', '30.122', '30.123'
    ],
    # San Juan Bautista, Ayolas, San Ignacio, San Miguel, San Patricio, Santa Maria, Santa Rosa Misiones,
    # Santiago, Villa Florida, Yabebyry
    '08 - MISIONES': [
        '30.124', '30.125', '30.126', '30.127', '30.128', '30.129', '30.130', '30.131', '30.132', '30.133'
    ],
    # Paraguari, Acahay, Caapucu, Gral. Bernardino Caballero, Carapegua, Escobar, La Colmena, Mbuyapey, Pirayu,
    # Quiindy, Quyquyho, San Roque Gonzalez De Santa Cruz, Sapucai, Tebicuarymi, Yaguaron, Ybycui, Ybytimi, Maria
    # Antonia
    '09 - PARAGUARI': [
        '30.134', '30.135', '30.136', '30.137', '30.138', '30.139', '30.140', '30.141', '30.142', '30.143',
        '30.144', '30.145', '30.146', '30.147', '30.148', '30.149', '30.150', '30.252'
    ],
    # Ciudad Del Este, Presidente Franco, Domingo Martinez De Irala, Juan Leon Mallorquin, Hernandarias, Santa Fe
    # Del Parana, Itakyry, Juan E. O'Leary, Nacunday, Yguazu, Los Cedrales, Minga Guazu, San Cristobal, Santa
    # Rita, Naranjal, Santa Rosa Del Monday, Minga Pora, Mbaracayu, San Alberto, Iruna, Tavapy, Dr. Raul Pena
    '10 - ALTO PARANA': [
        '30.151', '30.152', '30.153', '30.154', '30.155', '30.156', '30.157', '30.158', '30.159', '30.160',
        '30.161', '30.162', '30.163', '30.164', '30.165', '30.166', '30.167', '30.168', '30.169', '30.170',
        '30.240', '30.246'
    ],
    # Aregua, Capiata, Fernando De La Mora, Guarambare, Ita, Itaugua, Lambare, Limpio, Luque, Mariano Roque
    # Alonso, Nueva Italia, Nemby, San Antonio, San Lorenzo, Villa Elisa, Villeta, Ypacarai, Ypane, J. Augusto
    # Saldivar
    '11 - CENTRAL': [
        '30.171', '30.172', '30.173', '30.174', '30.175', '30.176', '30.177', '30.178', '30.179', '30.180',
        '30.181', '30.182', '30.183', '30.184', '30.185', '30.186', '30.187', '30.188', '30.189'
    ],
    # Pilar, Alberdi, Cerrito, Desmochados, Gral. Jose Eduvigis Diaz, Guazu Cua, Humaita, Isla Umbu, Laureles,
    # Mayor Jose D. Martinez, Paso De Patria, San Juan Bautista Neembucu, Tacuaras, Villa Franca, Villa Oliva,
    # Villalbin
    '12 - NEEMBUCU': [
        '30.190', '30.191', '30.192', '30.193', '30.194', '30.195', '30.196', '30.197', '30.198', '30.199',
        '30.200', '30.201', '30.202', '30.203', '30.204', '30.205'
    ],
    # Pedro Juan Caballero, Bella Vista - Amambay, Capitan Bado, Zanja Pyta, Karapai, Cerro Cora
    '13 - AMAMBAY': [
        '30.206', '30.207', '30.208', '30.242', '30.250', '30.259'
    ],
    # Salto Del Guaira, Corpus Christi, Villa Curuguaty, Yasy Cany, Villa Ygatimi, Itanara, Ypejhu, Gral.
    # Francisco Caballero Alvarez, Katuete, La Paloma Del Espiritu Santo, Nueva Esperanza, Yvyrarobana, Yby Pyta,
    # Maracana, Puerto Adela, Laurel
    '14 - CANINDEYU': [
        '30.209', '30.210', '30.211', '30.212', '30.213', '30.214', '30.215', '30.216', '30.217', '30.218',
        '30.219', '30.244', '30.247', '30.251', '30.255', '30.256'
    ],
    # Benjamin Aceval, Puerto Pinasco, Villa Hayes, Nanawa, Jose Falcon, Teniente 1° Manuel Irala Fernandez,
    # Teniente Esteban Martinez, General Jose Maria Bruguez, Campo Aceval, Municipalidad Nueva Asuncion
    '15 - PRESIDENTE HAYES': [
        '30.220', '30.221', '30.222', '30.223', '30.224', '30.229', '30.232', '30.236', '30.258', '30.262'
    ],
    # Jose Felix Estigarribia, Filadelfia, Loma Plata, Boqueron
    '16 - BOQUERON': [
        '30.228', '30.230', '30.231', '30.261'
    ],
    # Fuerte Olimpo, Puerto Casado, Bahia Negra, Carmelo Peralta
    '17 - ALTO PARAGUAY': [
        '30.225', '30.226', '30.227', '30.235'
    ],
}
MUNICIPALITY_DEPARTMENT = {code: department for department, codes in DEPARTMENT_MUNICIPALITIES.items() for code in codes}

# COMMAND ----------

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
df['hospital'] = flag(df.PROGRAM2.str.contains(HOSPITAL_PROGRAM2_KEYWORD, case=False, na=False), 1)

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
admin2_code = df.ADMIN2.str.strip().str[:6]
unmapped = sorted(set(admin2_code) - set(MUNICIPALITY_DEPARTMENT))
assert not unmapped, f'municipalities missing from DEPARTMENT_MUNICIPALITIES: {unmapped}'
df['geo'] = admin2_code.map(MUNICIPALITY_DEPARTMENT).map(normalize_cell)

df = df[['YEAR', 'ADMIN1', 'ADMIN2', 'ECON4', 'ECON5', 'ECON6', 'geo', 'approved', 'MODIFIED', 'COMMITTED', 'PAID']]
df.to_csv(f'{raw_microdata_csv_dir}/Municipalidades.csv', index=False, encoding='utf-8')
print(f'Municipalidades.csv: {len(df):,} rows')
print(df.groupby('YEAR').agg(rows=('YEAR', 'size'), modified=('MODIFIED', 'sum'), paid=('PAID', 'sum'),
                             municipalities=('ADMIN2', 'nunique'), departments=('geo', 'nunique')))
