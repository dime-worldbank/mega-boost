# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import os
import re
from glob import glob
from pathlib import Path
import pandas as pd
import numpy as np

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
COUNTRY = 'Albania'
if IS_DATABRICKS:
    # RAW_INPUT_DIR and prepare_raw_microdata_csv_dir come from %run ../utils
    raw_microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)
else:
    RAW_INPUT_DIR = os.environ.get('RAW_INPUT_DIR') or input("Enter the raw input directory (containing <country>/<year>/ subfolders): ").strip()
    raw_microdata_csv_dir = os.environ.get('OUTPUT_DIR') or input("Enter the output directory: ").strip()
    Path(raw_microdata_csv_dir).mkdir(parents=True, exist_ok=True)
ADMIN2_PAD_LENGTH = 3

col_format_map_7 = {
    "admin2": r"\d{3}",
    "admin3": r"\d{2}",
    "admin4": r"\d{7}",
    "fin_source": r"^\d{2}$",
    "func3": r"^[A-Za-z0-9]{5}$",
    "econ5": r"^\d{7}$",
    "admin5": r"\d{1,4}",
    "project": r"^[A-Za-z0-9]{1,7}$",
}

col_format_map_3 = {
    "admin2": r"\d{3}",
    "admin3": r"\d{2}",
    "admin4": r"\d{7}",
    "fin_source": r"\d{2}",
    "func3": r"^[A-Za-z0-9]{5}$",
    "econ3": r"\d{3,4}",
    "project": r"^[A-Za-z0-9]{7}$"
}

def validate_col_format(regex, col, df):
    assert df[col].astype(str).str.fullmatch(regex).all(), f"{col} does not follow {regex}"

# helper function to map to regions:
def map_to_region(admin2_item):
    loc_code = admin2_item[:3]
    if loc_code in [ '032', '042', '102', '124', '139', '140', '167', '302', '303', '304',
                     '305', '306', '307', '308', '309', '310', '311', '530', '531', '745', 
                     '746', '747', '835']:
        return 'Berat'
    elif loc_code in [ '025', '043', '046', '103', '106', '132',  '315', '316', '317', '318',
                       '319', '320', '321', '345', '346', '347', '348', '349', '350', '351',
                       '352', '353', '354', '355', '356', '357', '358', '645', '646', '647',
                       '648', '649', '650', '651', '652', '653', '654', '655', '755', '756',
                       '757', '758', '759', '760', '761', '762']:
        return 'Diber'
    elif loc_code in ['016', '047', '107', '108', '118', '119', '123', '150', '151', '163', 
                      '365', '366', '367', '368', '370', '372', '521', '522', '523', '524']:
        return 'Durres'
    elif loc_code in ['048', '109', '110', '114', '128', '134', '152', '153', '381', '382', 
                      '383', '384', '385', '386', '387', '388', '389', '390', '391', '392',
                      '393', '394', '395', '396', '397', '398', '399', '400', '430', '431',
                      '432', '433', '434', '435', '436', '437', '438', '586', '587', '588',
                      '589', '590', '591', '592', '593', '594', '680', '681', '682', '683',
                      '684', '742', '743', '744', '784', '841']:
        return 'Elbasan' 
    elif loc_code in [ '024', '049', '111', '112', '113', '129', '131', '147', '410', '411',
                       '412', '413', '414', '415', '416', '417', '418', '419', '420', '421',
                       '422', '423', '485', '486', '600', '601', '602', '603', '604', '605',
                       '606', '607', '608', '609', '610', '612', '613', '614', '635', '636',
                       '637', '638', '639', '640', '641', '642', '780', '836', '837']:
        return 'Fier'
    elif loc_code in ['011', '028', '034', '115', '116', '135', '142', '143', '154', '445',
                      '446', '447', '448', '449', '450', '451', '452', '453', '454', '455',
                      '690', '691', '693', '694', '695', '697', '781', '782', '783', '786',
                      '787', '838']:
        return 'Gjirokaster'
    elif loc_code in ['014', '015', '029', '045', '105', '120', '121', '122', '136', '168',
                      '335', '336', '337', '338', '487', '488', '489', '490', '496', '497',
                      '498', '499', '500', '501', '502', '503', '504', '505', '506', '507',
                      '508', '509', '696', '705', '706', '843', '844', '845', '846']:
        return 'Korce'
    elif loc_code in ['012', '018', '036', '117', '125', '145', '460', '461', '462', '540', 
                      '541', '542', '543', '544', '545', '546', '547', '548', '549', '550',
                      '551', '552', '553', '700', '701', '702', '703', '718', '704', '719',
                      '720', '721', '722', '723', '740', '741', '820', '823', '824', '825',
                      '826']:
        return 'Kukes' 
    elif loc_code in ['020', '026', '126', '127', '133', '162', '164', '560', '562', '570',
                      '571', '572', '573', '574', '575', '576', '577', '578', '625', '666',
                      '667', '668', '669', '670']:
        return 'Lezhe' 
    elif loc_code in ['033', '130', '137', '141', '155', '157', '626', '627', '628', '629',
                      '764', '765', '766', '767', '768', '769', '822']: 
        return 'Shkoder'
    elif loc_code in ['035', '101', '165', '166', '470', '471', '472', '473', '474', '475',
                      '476', '477', '715', '716', '770', '785', '795', '796', '797', '798',
                      '799', '800', '801', '802', '803', '804', '805', '807', '808', '809',
                      '811', '812', '821']: 
        return 'Tirane'
    elif loc_code in ['037', '044', '104', '138', '146', '156', '158', '159', '160', '325',
                      '326', '328', '730', '731', '732', '734', '735', '736', '737']:
        return 'Vlore'
    return 'Central'

def pad_left(code, length = 3):
    code = str(code).split('.')[0]
    while len(code)<length:
        code = '0'+code
    return code
        
def format_float(x):
    if pd.isna(x):
        return x
    x = str(x).strip()
    try:
        return float(x)
    except ValueError:
        pass
    euro_thousands_pattern = r'^\d{1,3}(\.\d{3})+,\d{2}$'
    if re.match(euro_thousands_pattern, x):
        try:
            x = x.replace('.', '').replace(',', '.')
            return float(x)
        except ValueError:
            return pd.NA
    comma_decimal_pattern = r'^\d+,\d{1,2}$'
    if re.match(comma_decimal_pattern, x):
        try:
            x = x.replace(',', '.')
            return float(x)
        except ValueError:
            return pd.NA
    return pd.NA

# COMMAND ----------

# The raw multi-file format is only used from 2023 onward; earlier years come through the
# legacy single-workbook path (see ALB_extract_microdata_excel_to_csv).
RAW_DATA_START_YEAR = 2023
years = sorted(
    year for year in (
        int(os.path.basename(year_dir))
        for year_dir in glob(f'{RAW_INPUT_DIR}/{COUNTRY}/[0-9][0-9][0-9][0-9]')
        if os.path.isdir(year_dir)
    )
    if year >= RAW_DATA_START_YEAR
)
assert years, f"No year subdirectories >= {RAW_DATA_START_YEAR} found under {RAW_INPUT_DIR}/{COUNTRY}"

# COMMAND ----------

col_names_3_digit = [
    'admin2', 'admin3', 'admin4', 'fin_source', 'func3', 'econ3', 'admin5', 'project', 'executed', 'revised', 'approved']
col_names_7_digit = [
    'admin2', 'admin3', 'admin4', 'fin_source', 'func3', 'econ5', 'admin5', 'project', 'executed']
rev_col_names_7_digit = ['admin2', 'admin3', 'admin4', 'econ5', 'admin5', 'executed']

# COMMAND ----------

# Two raw formats are supported per year: multiple files (one per category, as in 2023-2024) or
# a single consolidated workbook with one sheet per category (as in 2025 onward). The read_*
# helpers turn either into the same canonical columns; the finalize_* steps are then shared.

def _sheet_body(file, sheet):
    """Locate the 'Gov' header row of a consolidated-workbook sheet and return its data rows
    (first column all-digit, so title/total rows are dropped) together with the header labels,
    both trimmed to the columns that carry data."""
    raw = pd.read_excel(file, sheet_name=sheet, header=None, dtype=str)
    header_idx = next((i for i in range(len(raw))
                       if (raw.iloc[i].astype(str).str.strip() == 'Gov').any()), None)
    assert header_idx is not None, f"no 'Gov' header row in sheet {sheet!r} of {file}"
    header_row = raw.iloc[header_idx]
    body = raw.iloc[header_idx + 1:]
    body = body[body[0].astype(str).str.fullmatch(r'\d+')]
    populated = [
        c for c in body.columns
        if pd.notna(header_row[c]) or body[c].notna().any()
    ]
    keep = list(range(max(populated) + 1))
    header = [str(header_row[c]).strip() for c in keep]
    body = body[keep]
    body.columns = range(body.shape[1])
    return body.reset_index(drop=True), header

def _account_col(header, file, sheet):
    """Index of the economic-code ('Account') column that every data sheet carries. Raising here
    rather than guessing makes a missing/renamed column fail loudly instead of misclassifying."""
    lowered = [h.lower() for h in header]
    assert 'account' in lowered, f"sheet {sheet!r} of {file} has no 'Account' column to classify on"
    return lowered.index('account')

def classify_single_file_sheets(file):
    """Map each category ('7 digit', '3 digit', 'rev', '46655') to its sheet in the consolidated
    workbook. Classification is anchored on the economic-code ('Account') column: the 6-column
    revenue/46655 sheets are told apart by its 46655 prefix, and the 7- vs 3-digit expense sheets
    by its code width (7 digits vs 3-4)."""
    categories = {}
    for sheet in pd.ExcelFile(file).sheet_names:
        try:
            body, header = _sheet_body(file, sheet)
        except AssertionError:
            continue  # not a data sheet
        account = body[_account_col(header, file, sheet)].astype(str)
        if body.shape[1] <= 6:
            is_46655 = account.str.startswith('46655').mean() > 0.5
            categories['46655' if is_46655 else 'rev'] = sheet
        else:
            is_7_digit = account.str.fullmatch(r'\d{7}').mean() > 0.5
            categories['7 digit' if is_7_digit else '3 digit'] = sheet
    missing = {'7 digit', '3 digit', 'rev', '46655'} - set(categories)
    assert not missing, f"consolidated workbook {file} missing sheets for {sorted(missing)}"
    return categories

def read_expense_7_multi(f):
    sheet_name = pd.ExcelFile(f).sheet_names[-1]
    df_7 = pd.read_excel(f, sheet_name = sheet_name)
    header_idx = df_7.apply(lambda x: x.notna().sum(), axis=1).gt(5).idxmax()
    df_7.columns = df_7.iloc[header_idx]
    df_7 = df_7[header_idx+1:]
    df_7 = df_7[[col for col in df_7.columns if 'description' not in col.lower()]]
    assert df_7.shape[1] == 9
    df_7.columns = col_names_7_digit
    return df_7

def read_expense_7_single(file, sheet):
    body, _ = _sheet_body(file, sheet)
    assert body.shape[1] == 11, f"expected 11 columns in the 7-digit sheet, found {body.shape[1]}"
    # the consolidated 7-digit layout interleaves two description columns (institution name at
    # index 3, account description at index 7); drop them by position to leave the 9 canonical
    # columns, independent of the description text
    return body.drop(columns=[3, 7]).set_axis(col_names_7_digit, axis=1)

def finalize_expense_7(df_7, year):
    df_7 = df_7[df_7.admin2.notna()]
    df_7 = df_7.dropna(how='all')
    for col, regex in col_format_map_7.items():
        validate_col_format(regex, col, df_7)
    df_7 = df_7.astype({col:'str' for col in df_7.columns if col!='executed'})
    df_7['executed'] = df_7['executed'].map(format_float)
    df_7['econ3'] = df_7['econ5'].str[:3]
    df_7['year'] = year
    df_7['src'] = '7 digit'
    return df_7

def read_expense_3_multi(f):
    sheet_names = pd.ExcelFile(f).sheet_names[-2:]
    return pd.concat([
        d[d[d.columns[0]].astype(str).str.isdigit()].set_axis(col_names_3_digit, axis=1)
        for d in pd.read_excel(f, sheet_name=sheet_names, dtype=str).values()
        ], axis=0, ignore_index=True)

def read_expense_3_single(file, sheet):
    body, _ = _sheet_body(file, sheet)
    assert body.shape[1] == 11
    body.columns = col_names_3_digit
    return body

def finalize_expense_3(df_3, year):
    float_cols = ['executed', 'revised', 'approved']
    df_3[float_cols] = df_3[float_cols].applymap(format_float)
    df_3.drop_duplicates(inplace=True)
    df_3['executed'] = np.nan
    df_3.dropna(how='all', inplace=True)

    for col, regex in col_format_map_3.items():
        validate_col_format(regex, col, df_3)

    df_3['year'] = year
    df_3['src'] = '3 digit'
    df_3 = df_3[df_3.econ3.map(lambda x: len(str(x))==3)]
    return df_3

def read_rev_multi(f):
    df = pd.read_excel(f, dtype=str)
    assert df.shape[1] == 6
    df.columns = rev_col_names_7_digit
    return df

def read_rev_single(file, sheet):
    body, _ = _sheet_body(file, sheet)
    assert body.shape[1] == 6
    body.columns = rev_col_names_7_digit
    return body

def finalize_rev(df, src):
    df = df[df.admin2.map(lambda x: str(x).isdigit())]
    df['executed'] = df.executed.astype('float')
    df['src'] = src
    return df

# COMMAND ----------

for year in years:
    outfile = f'{raw_microdata_csv_dir}/{year}.csv'
    if os.path.exists(outfile):  # skip years already extracted; delete the CSV to force a refresh
        continue
    year_files = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/{year}/*.xlsx')

    if len(year_files) == 1:  # single consolidated workbook (2025 onward)
        src_file = year_files[0]
        sheets = classify_single_file_sheets(src_file)
        df_7 = read_expense_7_single(src_file, sheets['7 digit'])
        df_3 = read_expense_3_single(src_file, sheets['3 digit'])
    else:  # one file per category (2023-2024)
        expense_data_files = [f for f in year_files if 'ex' in os.path.basename(f).lower() and 'rev' not in os.path.basename(f).lower()]
        seven_digit_files = [f for f in expense_data_files if '7 digit' in os.path.basename(f).lower()]
        three_digit_files = [f for f in expense_data_files if '3 digit' in os.path.basename(f).lower()]
        assert len(seven_digit_files) == 1, f"Expected exactly one '7 digit' file, found {len(seven_digit_files)}"
        assert len(three_digit_files) == 1, f"Expected exactly one '3 digit' file, found {len(three_digit_files)}"
        df_7 = read_expense_7_multi(seven_digit_files[0])
        df_3 = read_expense_3_multi(three_digit_files[0])

    df_7 = finalize_expense_7(df_7, year)
    df_3 = finalize_expense_3(df_3, year)
    df = pd.concat([df_7, df_3], ignore_index=True)
    df['counties'] = df.admin2.map(lambda x: map_to_region(pad_left(str(x).split('.')[0], length=ADMIN2_PAD_LENGTH)))
    df.to_csv(outfile, index=False)

# COMMAND ----------

# Revenue data extraction into CSV

for year in years:
    outfile = f'{raw_microdata_csv_dir}/{year}_rev.csv'
    if os.path.exists(outfile):  # skip years already extracted; delete the CSV to force a refresh
        continue
    year_files = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/{year}/*.xlsx')

    if len(year_files) == 1:  # single consolidated workbook (2025 onward)
        src_file = year_files[0]
        sheets = classify_single_file_sheets(src_file)
        df_7_rev = read_rev_single(src_file, sheets['rev'])
        df_46655 = read_rev_single(src_file, sheets['46655'])
    else:  # one file per category (2023-2024)
        revenue_data_files = [f for f in year_files if any(y in os.path.basename(f).lower() for y in ['rev', '46655'])]
        rev_files = [f for f in revenue_data_files if 'rev' in os.path.basename(f).lower()]
        acc_files = [f for f in revenue_data_files if '46655' in os.path.basename(f).lower()]
        assert len(rev_files) == 1 and len(acc_files) == 1, \
            f"Expected one 'rev' and one '46655' file, found {len(rev_files)} and {len(acc_files)}"
        df_7_rev = read_rev_multi(rev_files[0])
        df_46655 = read_rev_multi(acc_files[0])

    df_7_rev = finalize_rev(df_7_rev, '7 digit rev')
    df_46655 = finalize_rev(df_46655, '46655')
    df = pd.concat([df_7_rev, df_46655], ignore_index=True)
    df['year'] = year
    df.to_csv(outfile, index=False)
