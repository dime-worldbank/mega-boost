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

# Workbook author decisions (2026-10-21); see README
EXCLUDE_CENTRAL_BORROWING = True
PREDIAL_CATEGORY = ('Income Tax', None)

CUADROS_UNIT = 1e9  # MinHacienda Cuadros are in miles de millones de pesos
CUADROS_APPROVED_COL, CUADROS_EXECUTED_COL = 3, 4
CUADROS_SHEETS = ['C1', 'C2', 'C3', 'C4', 'C5']
TOTAL_TOLERANCE = 1e6
CENTRAL_BORROWING_LINES = {'recursos de credito interno', 'recursos de credito externo'}
# Only this C1 block is read; Nación rows come from C2-C5
C1_BLOCK = 'ingresos de los establecimientos publicos'

# Frozen 2019-2022 labels were OCR'd from a scanned PDF
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
    """Precedence: (sheet, label), then ('*', label), then (sheet, '*'); None if unmatched."""
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
    # "Concepto" moves between header rows across years; the column-number row is stable
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


SUBNATIONAL_COLUMNS = ['year', 'codigo_fut', 'entity_name', 'concept_code', 'econ1', 'econ2', 'econ3', 'econ4',
                       'approved', 'executed', 'econ_rev', 'econ_rev_sub', 'exclude_from_total']
SUBNATIONAL_ROOT_CODES = {'TI', '1'}
SUBNATIONAL_FIRST_YEAR = 2019
SUBNATIONAL_SHEETS = ['Raw', 'Ejecución de Ingresos', 'Programación de Ingresos']
SUBNATIONAL_RELATIVE_TOLERANCE = 1e-4
# Departments (11…) and municipalities (21…); 2021 files list every public entity
TERRITORIAL_FUT_PREFIXES = ('11', '21')
# Header names vary across the 2019-2024 layouts
CODE_HEADERS = {'codigoconcepto', 'conceptocod', 'codigoconceptocod'}
NAME_HEADERS = ['concepto', 'nombreconcepto']
ENTITY_HEADERS = ['codigofut', 'entidadterritorialid', 'nombreentidad']
# 2021's EntidadTerritorialID holds '110505000 Antioquia', the short name used in later years
ENTITY_NAME_HEADERS = ['entidad', 'nombreentidad', 'entidadterritorialid', 'nombrefut']
EXECUTED_HEADERS = ['totalrecaudo', 'totalingresos']  # FUT Total Ingresos includes sin situación de fondos
APPROVED_HEADERS = ['presupuestodefinitivo']


PREDIAL = 'predial'
EXCLUDED = 'excluded'
# CUIPO (2021+) and FUT (2019-2020) code prefixes; longest match wins
SUBNATIONAL_RULES = {
    '1.1.01.01.200': PREDIAL, 'TI.A.1.3': PREDIAL,
    '1.1.01.02.109': ('Excises', 'Excises (Fuels)'), 'TI.A.1.26': ('Excises', 'Excises (Fuels)'),
    '1.1.01.02.106': ('Excises', 'Excises (Tobacco)'), 'TI.A.1.18': ('Excises', 'Excises (Tobacco)'),
    **{c: ('Excises', 'Excises (Alcohol)') for c in
       ['1.1.01.02.103', '1.1.01.02.104', '1.1.01.02.105', 'TI.A.1.14', 'TI.A.1.15', 'TI.A.1.16', 'TI.A.1.17']},
    **{c: ('Permits and Fees', None) for c in
       ['1.1.01.01.100', '1.1.01.02.300', '1.1.02.02', '1.1.02.03', 'TI.A.1.2', 'TI.A.1.28', 'TI.A.2.1', 'TI.A.2.2']},
    # Transfers and financing: out of total revenue to avoid double counting
    **{c: EXCLUDED for c in
       ['1.1.02.06', '1.2.06', '1.2.07', '1.2.08', '1.2.10', '1.2.12', 'TI.A.2.6', 'TI.B.1', 'TI.B.4', 'TI.B.6', 'TI.B.10']},
}


def classify_subnational(code):
    matches = [p for p in SUBNATIONAL_RULES if code == p or code.startswith(p + '.')]
    rule = SUBNATIONAL_RULES[max(matches, key=len)] if matches else None
    if rule == EXCLUDED:
        return None, None, True
    if rule == PREDIAL:
        return PREDIAL_CATEGORY[0], PREDIAL_CATEGORY[1], False
    if rule is None:
        return 'Subnational Own Revenues', None, False
    return rule[0], rule[1], False


def header_key(v):
    return normalize_label(v).replace(' ', '') if isinstance(v, str) else ''


def read_subnational_sheet(path):
    xl = pd.ExcelFile(path)
    sheet = next((s for s in SUBNATIONAL_SHEETS if s in xl.sheet_names), xl.sheet_names[0])
    head = pd.read_excel(path, sheet_name=sheet, header=None, nrows=40, dtype=str)
    header_row = next(i for i, row in head.iterrows() if any(header_key(v) in CODE_HEADERS for v in row))
    df = pd.read_excel(path, sheet_name=sheet, header=header_row, dtype=str)
    keys = [header_key(c.split('.')[0] if re.search(r'\.\d+$', c) else c) for c in df.columns]

    def col(candidates, after=None):
        for cand in candidates:
            if cand in keys:
                return df.iloc[:, keys.index(cand)]
        # 2021 names the concept column just 'Nombre', right after the code
        return df.iloc[:, after + 1] if after is not None else None

    code_idx = next(i for i, k in enumerate(keys) if k in CODE_HEADERS)
    code = df.iloc[:, code_idx].astype(str).str.strip()
    name = col(NAME_HEADERS, after=code_idx).astype(str).str.strip()
    name = pd.Series([n[len(c):].strip() if n.startswith(c + ' ') else n for c, n in zip(code, name)], index=df.index)
    entity_name = (col(ENTITY_NAME_HEADERS).astype(str).str.strip().str.replace(r'^\d{9}\s*', '', regex=True)
                   .replace('', None))
    executed, approved = col(EXECUTED_HEADERS), col(APPROVED_HEADERS)
    return pd.DataFrame({
        'codigo_fut': col(ENTITY_HEADERS).astype(str).str.extract(r'^\s*(\d+)', expand=False),
        'entity_name': entity_name, 'concept_code': code, 'concept_name': name,
        'executed': pd.to_numeric(executed, errors='coerce') if executed is not None else float('nan'),
        'approved': pd.to_numeric(approved, errors='coerce') if approved is not None else float('nan'),
    })


def leaf_rows(df):
    df = df[df.concept_code != 'VAL']
    df = df[~df.concept_code.isin(SUBNATIONAL_ROOT_CODES)]
    parents = set()
    for (year, entity), codes in df.groupby(['year', 'codigo_fut']).concept_code:
        for c in set(codes):
            parts = c.split('.')
            parents.update((year, entity, '.'.join(parts[:i])) for i in range(1, len(parts)))
    is_parent = [(y, e, c) in parents for y, e, c in zip(df.year, df.codigo_fut, df.concept_code)]
    leaves = df[[not p for p in is_parent]]
    return (leaves.groupby(['year', 'codigo_fut', 'concept_code'], as_index=False)
            .agg(entity_name=('entity_name', 'first'),
                 approved=('approved', lambda s: s.sum(min_count=1)),
                 executed=('executed', lambda s: s.sum(min_count=1))))


def read_subnational(year, ejecucion_path, programacion_path):
    raw = read_subnational_sheet(ejecucion_path)
    if programacion_path is not None:
        raw = pd.concat([raw.drop(columns='approved'), read_subnational_sheet(programacion_path).drop(columns='executed')])
    raw['year'] = year
    # 2021's 8-digit '21…' codes are regional corporations, not municipalities
    raw = raw[(raw.codigo_fut.str.len() == 9) & raw.codigo_fut.str.startswith(TERRITORIAL_FUT_PREFIXES)]
    names = raw.drop_duplicates('concept_code').set_index('concept_code').concept_name.to_dict()
    entity_names = raw.dropna(subset=['entity_name']).groupby('codigo_fut').entity_name.first()
    root = (raw[raw.concept_code.isin(SUBNATIONAL_ROOT_CODES)]
            .groupby(['year', 'codigo_fut'], as_index=False)[['approved', 'executed']].sum(min_count=1))
    df = leaf_rows(raw)
    df['entity_name'] = df.codigo_fut.map(entity_names)

    def labelled(code, depth):
        parts = code.split('.')
        if len(parts) < depth:
            return None
        prefix = '.'.join(parts[:depth])
        return f'{prefix} {names.get(prefix, "")}'.strip()

    for i, depth in enumerate([2, 3, 4], start=1):
        df[f'econ{i}'] = df.concept_code.map(lambda c: labelled(c, depth) if len(c.split('.')) > depth else None)
    df['econ4'] = df.concept_code.map(lambda c: f'{c} {names.get(c, "")}'.strip())

    df[['econ_rev', 'econ_rev_sub', 'exclude_from_total']] = pd.DataFrame(
        df.concept_code.map(classify_subnational).tolist(), index=df.index)

    sums = df.groupby('codigo_fut')[['approved', 'executed']].sum(min_count=1)
    check = root.drop(columns='year').set_index('codigo_fut').join(sums, rsuffix='_leaves')
    for measure in ['approved', 'executed']:
        gap = (check[measure] - check[f'{measure}_leaves']).fillna(0)
        mismatched = check[gap.abs() > 1e3]
        if not mismatched.empty:
            print(f'{year} {measure}: {len(mismatched)} entities whose leaf rows differ from their root row\n{mismatched}')
        assert abs(gap.sum()) <= SUBNATIONAL_RELATIVE_TOLERANCE * check[measure].sum(), \
            f'{year} {measure}: leaf rows do not add up to the root rows (gap {gap.sum()} of {check[measure].sum()})'
    return df[SUBNATIONAL_COLUMNS], root


def subnational_files(ingresos_dir):
    """{year: (ejecucion_path, programacion_path | None)} for years >= SUBNATIONAL_FIRST_YEAR."""
    files = {}
    for path in sorted(glob(f'{ingresos_dir}/*.xlsx')):
        name = normalize_label(Path(path).stem)
        year = int(re.match(r'(\d{4})', name).group(1)) if re.match(r'\d{4}', name) else None
        if year is None or year < SUBNATIONAL_FIRST_YEAR:
            continue
        ejec, prog = files.get(year, (None, None))
        if 'futingresos' in name.replace(' ', ''):
            ejec = path
        elif 'ejecucion' in name:
            ejec = path
        elif 'programacion' in name or 'approved' in name:
            prog = path
        files[year] = (ejec, prog)
    for year, (ejec, _) in files.items():
        assert ejec is not None, f'No ejecución file found for subnational {year} in {ingresos_dir}'
    return files


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

    for year, (ejecucion_path, programacion_path) in subnational_files(f'{RAW_INPUT_DIR}/{COUNTRY}/subnational/ingresos').items():
        df, root = read_subnational(year, ejecucion_path, programacion_path)
        print(year, df.shape, ejecucion_path, programacion_path)
        df.to_csv(f'{microdata_csv_dir}/revenue_subnational_{year}.csv', index=False)
        root.to_csv(f'{microdata_csv_dir}/revenue_subnational_root_{year}.csv', index=False)
