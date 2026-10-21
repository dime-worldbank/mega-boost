# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import os
import re
import unicodedata
from glob import glob
from pathlib import Path

import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
COUNTRY = 'Colombia'

# Open questions with the workbook author; see Colombia/README.md "Revenue pipeline"
EXCLUDE_CENTRAL_BORROWING = False
PREDIAL_CATEGORY = ('Property Taxes', 'Property Taxes (Immovable)')

CUADROS_UNIT = 1e9  # MinHacienda Cuadros are in miles de millones de pesos
CUADROS_APPROVED_COL, CUADROS_EXECUTED_COL = 3, 4
CUADROS_SHEETS = ['C1', 'C2', 'C3', 'C4', 'C5']
TOTAL_TOLERANCE = 1e6
CENTRAL_BORROWING_LINES = {'recursos de credito interno', 'recursos de credito externo'}
# C1 is only read for this block; the Nación rows come from C2-C5
C1_BLOCK = 'ingresos de los establecimientos publicos'

# The frozen 2019-2022 central lines were OCR'd from a scanned PDF
OCR_WORD_FIXES = {'Oe': 'De', 'Naclonal': 'Nacional', 'Pijblicos': 'Públicos',
                  'Rjqljeza': 'Riqueza', 'O8': '08', 'PatriMonio': 'Patrimonio'}
OCR_TRUNCATED_ENDINGS = {'Aduanas Y': 'Aduanas Y Recargos'}

CENTRAL_COLUMNS = ['year', 'econ1', 'econ2', 'econ3', 'econ4', 'approved', 'executed',
                   'econ_rev', 'econ_rev_sub', 'exclude_from_total']


def normalize_label(s):
    s = re.sub(r'\s+\d+/$', '', str(s).strip())
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode().lower()
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', s).split())


def strip_code_prefix(label):
    return re.sub(r'^\d+(\.\d+)*\s+', '', label)


def load_line_map(path='./central_revenue_lines.csv'):
    return pd.read_csv(path, dtype=str).fillna('')


def lookup_line(line_map, sheet, label):
    """Resolve a central line: sheet-specific row, then a '*'-sheet row for the label,
    then the sheet's '*' default. Returns None when nothing matches."""
    def row(s, l):
        hit = line_map[(line_map.sheet == s) & (line_map.label == l)]
        return hit.iloc[0] if len(hit) else None

    specific, any_sheet, default = row(sheet, label), row('*', label), row(sheet, '*')
    match = next((r for r in (specific, any_sheet, default) if r is not None), None)
    if match is None:
        return None
    category = next((r for r in (specific, any_sheet) if r is not None and r.econ_rev), match)
    return {
        'is_subtotal': match.is_subtotal == '1',
        'econ1': match.econ1 or (default.econ1 if default is not None else ''),
        'econ2': match.econ2, 'econ3': match.econ3,
        'econ_rev': category.econ_rev or None, 'econ_rev_sub': category.econ_rev_sub or None,
    }


def exclude_from_total(label):
    return EXCLUDE_CENTRAL_BORROWING and label in CENTRAL_BORROWING_LINES


def read_cuadros_sheet(path, sheet_name):
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)
    labels = df[0].map(lambda v: normalize_label(v) if isinstance(v, str) else '')
    # "Concepto" moves between the two header rows across years; the column-number row doesn't
    numbers_row = df[df[CUADROS_APPROVED_COL].astype(str).str.replace(' ', '') == '(3)=(1+2)'].index[0]
    approved_hdr = normalize_label(df.iloc[numbers_row - 1, CUADROS_APPROVED_COL])
    executed_hdr = normalize_label(df.iloc[numbers_row - 2, CUADROS_EXECUTED_COL])
    assert approved_hdr in ('definitivo', 'vigente') and executed_hdr.startswith('recaudo'), \
        f'Unexpected Cuadros header in {sheet_name}: {approved_hdr!r}, {executed_hdr!r}'
    body = df.iloc[numbers_row + 1:]
    approved = pd.to_numeric(body[CUADROS_APPROVED_COL], errors='coerce')
    executed = pd.to_numeric(body[CUADROS_EXECUTED_COL], errors='coerce')
    keep = body[0].map(lambda v: isinstance(v, str)) & (approved.notna() | executed.notna())
    return pd.DataFrame({
        'raw_label': body[0][keep].map(lambda v: re.sub(r'\s+\d+/$', '', ' '.join(v.split()))),
        'label': labels[body.index][keep],
        'approved': approved[keep].fillna(0) * CUADROS_UNIT,
        'executed': executed[keep].fillna(0) * CUADROS_UNIT,
    })


def parse_cuadros(path, line_map):
    xl = pd.ExcelFile(path)
    year = None
    lines = []
    for sheet in CUADROS_SHEETS:
        sheet_name = next(s for s in xl.sheet_names if s.split()[0] == sheet)
        if year is None:
            text = ' '.join(pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=10)
                            .astype(str).values.ravel())
            year = int(re.search(r'diciembre de (\d{4})', text, re.IGNORECASE).group(1))
        rows = read_cuadros_sheet(path, sheet_name)
        hierarchy = {'econ1': '', 'econ2': '', 'econ3': ''}
        expected_total, sheet_lines = None, []
        for r in rows.itertuples():
            if r.label.startswith('total ') and sheet != 'C1':
                expected_total = (r.approved, r.executed)
                continue
            line = lookup_line(line_map, sheet, r.label)
            if line is None:
                raise ValueError(f'Unmapped central revenue line in {sheet}: {r.label}')
            if line['is_subtotal']:
                if line['econ1'] or line['econ2']:
                    hierarchy = {'econ1': line['econ1'] or hierarchy['econ1'], 'econ2': line['econ2'], 'econ3': ''}
                if line['econ3']:
                    hierarchy['econ3'] = line['econ3']
                if sheet == 'C1' and r.label == C1_BLOCK:
                    expected_total = (r.approved, r.executed)
                continue
            if sheet == 'C1' and hierarchy['econ1'] == '':
                continue
            sheet_lines.append({
                'year': year,
                'econ1': hierarchy['econ1'] or line['econ1'], 'econ2': hierarchy['econ2'],
                'econ3': hierarchy['econ3'], 'econ4': r.raw_label,
                'approved': r.approved, 'executed': r.executed,
                'econ_rev': line['econ_rev'], 'econ_rev_sub': line['econ_rev_sub'],
                'exclude_from_total': exclude_from_total(r.label),
            })
        assert expected_total is not None, f'No total row found in {sheet} of {path}'
        for i, measure in enumerate(['approved', 'executed']):
            parsed = sum(l[measure] for l in sheet_lines)
            assert abs(parsed - expected_total[i]) < TOTAL_TOLERANCE, \
                f'{sheet} {measure}: lines sum to {parsed}, total row says {expected_total[i]}'
        lines += sheet_lines
    return pd.DataFrame(lines, columns=CENTRAL_COLUMNS).replace('', None)


def fix_ocr_label(label):
    if not isinstance(label, str):
        return label
    for old, new in OCR_WORD_FIXES.items():
        label = re.sub(rf'\b{old}\b', new, label)
    for old, new in OCR_TRUNCATED_ENDINGS.items():
        if label.endswith(old):
            label = label[:-len(old)] + new
    return label


def tag_frozen(frozen, line_map):
    df = frozen.drop(columns='admin0').copy()
    for col in ['econ1', 'econ2', 'econ3', 'econ4']:
        df[col] = df[col].map(fix_ocr_label)
    categories = line_map[line_map.sheet.isin(['*', 'FROZEN'])].set_index('label')

    def category(row):
        leaf = next(v for v in (row.econ4, row.econ3, row.econ2) if isinstance(v, str))
        label = normalize_label(strip_code_prefix(leaf))
        if label in categories.index:
            hit = categories.loc[label]
            return pd.Series([hit.econ_rev or None, hit.econ_rev_sub or None, exclude_from_total(label)])
        return pd.Series([None, None, exclude_from_total(label)])

    df[['econ_rev', 'econ_rev_sub', 'exclude_from_total']] = df.apply(category, axis=1)
    return df[CENTRAL_COLUMNS]


if __name__ == '__main__':
    if IS_DATABRICKS:
        # RAW_INPUT_DIR and prepare_raw_microdata_csv_dir come from %run ../utils
        microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)
    else:
        RAW_INPUT_DIR = os.environ.get('RAW_INPUT_DIR') or input('Enter the raw input directory: ').strip()
        microdata_csv_dir = os.environ.get('OUTPUT_DIR') or input('Enter the output directory: ').strip()
        Path(microdata_csv_dir).mkdir(parents=True, exist_ok=True)

    line_map = load_line_map()

    frozen = pd.read_csv(f'{RAW_INPUT_DIR}/{COUNTRY}/auxiliary/central_revenue_2019_2022.csv')
    for year, df in tag_frozen(frozen, line_map).groupby('year'):
        df.to_csv(f'{microdata_csv_dir}/revenue_central_{year}.csv', index=False)

    for path in sorted(glob(f'{RAW_INPUT_DIR}/{COUNTRY}/central/revenues/*INGRESOS PGN DICIEMBRE*.xlsx')):
        df = parse_cuadros(path, line_map)
        year = df.year.iloc[0]
        print(year, df.shape, path)
        df.to_csv(f'{microdata_csv_dir}/revenue_central_{year}.csv', index=False)
