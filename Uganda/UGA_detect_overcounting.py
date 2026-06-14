"""Uganda BOOST overcounting detector + formula->Python validator.

Standalone pandas diagnostic (run locally, not on Databricks). It does two jobs, both driven
by the *actual* SUMIFS formulas parsed from the workbook's `Executed` sheet -- so it stays
faithful to Excel and is robust to the fact that 106/273 codes change criteria by year:

  1. VALIDATION (formula -> Python correctness). For every `EXP_*` code and every year column,
     parse that cell's SUMIFS, evaluate it against the Expenditure microdata, and check that
     our Sum(executed) reproduces the Excel cached cell value. Any mismatch is a parser/mapping
     bug to fix before trusting the overlap numbers.

  2. OVERCOUNT DETECTION (flat, per dimension, per year). One-hot encode each line against each
     category predicate (using the criteria variant for that line's fiscal year) and flag any
     line matched by >1 category WITHIN A FLAT DIMENSION (econ / econ_sub / func -- NO hierarchy
     assumed). Report, per overlapping code pair: #lines, Sum(executed) in the intersection, the
     years affected, and % of the dimension total.
     NOTE: `func_sub` overlap detection is intentionally OMITTED -- the func/func_sub COFOG
     hierarchy (e.g. Transport > Roads, Housing > Water&sanitation) is NOT yet agreed with the
     experts, so reporting func_sub overlaps would prejudge that structure. Re-enable by adding
     "func_sub" back to DIMENSIONS once the hierarchy is signed off.

Outputs (written next to this script, under _detect/):
    _detect/onehot_sample.csv        one-hot table (sampled) for inspection
    _detect/validation.csv           per (code, year) Excel-vs-Python reconciliation
    _detect/overlaps.csv             every overlapping code pair with magnitude
    _detect/overcounting_report.md   human-readable summary (seeds verification.md)

Usage:
    python UGA_detect_overcounting.py \
        --workbook "../temp/Uganda BOOST.xlsx" \
        --microdata /tmp/uganda_expenditure.csv \
        --out ./_detect
"""
import argparse
import os
import re
import zipfile
from xml.etree import ElementTree as ET

import pandas as pd

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# ----------------------------------------------------------------------------------------
# 1. Named-range -> Expenditure CSV column header (from the workbook's defined names).
#    The SUMIFS reference these named ranges; the CSV keeps the original Excel headers.
# ----------------------------------------------------------------------------------------
NAMED_RANGE_TO_HEADER = {
    "year": "Year",
    "budget_type": "Budget Type",
    "admin1": "MALGs",
    "admin2": "Vote",
    "admin3": "Program",
    "admin4": "Project",
    "geo": "geo1",
    "econ2": "Econ 2",
    "econ3": "Econ3",
    "econ5": "Item",
    "func0": "MTEF with External Debt and Arrears Adjusted",
    "Vote_Function": "Vote Function",
    "func1": "Func1",
    "func2": "Func2",
    "func3": "Func3",
    "approved": "Budget ",
    "executed": "Expenditure",
    "add": "add",
    "transfer": "Transfers",
    "SP": "social protection",
    "pension": "pension",
    "rail": "rail",
    "air": "air",
    "water": "water",
    "security": "security",
    "health": "health",
    "education": "education",
    "Tertiary": "Tertiary",
    "wss": "wss",
    "assistance": "assistance",
}

YEAR_COLS = list("CDEFGHIJKLMNOPQRSTU")  # Executed sheet year columns C..U (2005/06..2023/24)


# ----------------------------------------------------------------------------------------
# 2. Read the Executed sheet: per code, per year column, the formula string and cached value.
# ----------------------------------------------------------------------------------------
def _colrow(ref):
    m = re.match(r"([A-Z]+)(\d+)", ref)
    return m.group(1), int(m.group(2))


def _col_to_num(col):
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch) - 64)
    return n


def _num_to_col(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


_CELLREF = re.compile(r"(\$?)([A-Z]{1,3})(\$?)(\d+)")


def _shift_formula(text, dcol, drow):
    """Shift the *relative* A1 references in a formula by (dcol, drow), skipping quoted strings.

    Excel stores a shared formula's text only on the master cell; each slave cell's value is that
    formula with its relative references shifted by the slave's offset from the master (absolute
    `$`-anchored parts don't move). We resolve slaves by applying this shift. Named ranges
    (executed/year/SP/func0/econ2/Vote_Function...) are lowercase/underscored or have no trailing
    digit, so the uppercase-letters-then-digits cell-ref pattern never matches them.
    """
    parts = re.split(r'("[^"]*")', text)  # odd indices are quoted string literals -> leave as-is
    out = []
    for k, part in enumerate(parts):
        if k % 2 == 1:
            out.append(part)
            continue

        def repl(m):
            dc, col, dr, row = m.groups()
            ncol = col if dc == "$" else _num_to_col(_col_to_num(col) + dcol)
            nrow = row if dr == "$" else str(int(row) + drow)
            return f"{dc}{ncol}{dr}{nrow}"

        out.append(_CELLREF.sub(repl, part))
    return "".join(out)


def read_executed_sheet(xlsx_path):
    z = zipfile.ZipFile(xlsx_path)
    ss = []
    data = z.read("xl/sharedStrings.xml").decode("utf-8")
    for si in re.findall(r"<si>(.*?)</si>", data, re.S):
        txt = "".join(re.findall(r"<t[^>]*>(.*?)</t>", si, re.S))
        ss.append(re.sub(r"<[^>]+>", "", txt))
    root = ET.fromstring(z.read("xl/worksheets/sheet2.xml").decode("utf-8"))
    # First pass: read every cell's value + formula element, and index shared-formula MASTERS by si.
    # Excel writes the master (with `ref` + full text) before its slaves, but we resolve in a second
    # pass to be order-independent. A slave cell carries `<f t="shared" si="N"/>` with NO text and
    # inherits the master's formula, shifted by the slave's (col,row) offset from the master.
    cells = {}        # (col,row) -> (value, formula)   formula resolved (shared slaves included)
    shared = {}       # si -> (master_col, master_row, master_text)
    raw = []          # [(col, row, value, f_text, f_type, f_si)]
    for c in root.iter(f"{NS}c"):
        col, row = _colrow(c.get("r"))
        t = c.get("t")
        f = c.find(f"{NS}f")
        v = c.find(f"{NS}v")
        f_text = f.text if (f is not None and f.text) else None
        f_type = f.get("t") if f is not None else None
        f_si = f.get("si") if f is not None else None
        val = None
        if v is not None and v.text is not None:
            val = ss[int(v.text)] if t == "s" else v.text
        if f_type == "shared" and f_si is not None and f_text:
            shared[f_si] = (col, row, f_text)   # master cell of this shared range
        raw.append((col, row, val, f_text, f_type, f_si))
    # Second pass: resolve shared-formula slaves to their (shifted) master text.
    for col, row, val, f_text, f_type, f_si in raw:
        formula = f_text
        if formula is None and f_type == "shared" and f_si in shared:
            mcol, mrow, mtext = shared[f_si]
            formula = _shift_formula(mtext, _col_to_num(col) - _col_to_num(mcol), row - mrow)
        cells[(col, row)] = (val, formula)

    # year label per column (row 1)
    year_label = {col: cells.get((col, 1), (None, None))[0] for col in YEAR_COLS}

    codes = {}  # code -> {row, label, per_year: {year_label: {formula, cached}}}
    rows = sorted({r for (_, r) in cells})
    for r in rows:
        code = cells.get(("A", r), (None, None))[0]
        if not code or not str(code).startswith("EXP"):
            continue
        label = cells.get(("B", r), (None, None))[0]
        per_year = {}
        for col in YEAR_COLS:
            val, formula = cells.get((col, r), (None, None))
            yl = year_label[col]
            cached = None
            if val not in (None, "", ".."):
                try:
                    cached = float(val)
                except (TypeError, ValueError):
                    cached = None
            per_year[yl] = {"formula": formula, "cached": cached, "col": col}
        codes[code] = {"row": r, "label": label, "per_year": per_year}
    return codes, year_label


# ----------------------------------------------------------------------------------------
# 3. A small SUMIFS interpreter: parse a formula into summable terms, evaluate to masks/values.
# ----------------------------------------------------------------------------------------
def _split_top_commas(s):
    parts, depth, inq, cur = [], 0, False, ""
    for ch in s:
        if ch == '"':
            inq = not inq
            cur += ch
        elif inq:
            cur += ch
        elif ch in "({":
            depth += 1
            cur += ch
        elif ch in ")}":
            depth -= 1
            cur += ch
        elif ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur != "":
        parts.append(cur)
    return parts


def _extract_sumifs(formula):
    """Return the inner-arg string of every SUMIFS(...) in the formula (balanced parens)."""
    res, i = [], 0
    tok = "SUMIFS("
    while True:
        k = formula.find(tok, i)
        if k < 0:
            break
        j = k + len(tok)
        depth = 1
        while j < len(formula) and depth > 0:
            if formula[j] == "(":
                depth += 1
            elif formula[j] == ")":
                depth -= 1
            j += 1
        res.append(formula[k + len(tok): j - 1])
        i = j
    return res


def _residual_signed_refs(formula):
    """Top-level `+CELL` / `-CELL` references that sit OUTSIDE any SUMIFS (composite formulas).

    A few codes are defined by composition rather than a single SUMIFS, e.g.
        EXP_FUNC_HOU_EXE  (2005/06) = SUMIFS(func0="02 Lands..") + C205   (C205 = Water&sanitation)
        EXP_ECON_TOT_EXP_EXE (2005/06) = SUMIFS(year,C$1)        - C25    (C25  = debt repayment)
    `_extract_sumifs` only sees the inline SUMIFS, so the referenced cell is silently dropped.
    We blank out every SUMIFS(...) block first (their internal refs like C$1/T$1 go with them),
    then read the signed cell tokens that remain. Returns [(sign, col, row), ...]; row==1 (the
    year header row) is ignored as a safety guard.
    """
    s = formula
    for inner in _extract_sumifs(formula):
        s = s.replace("SUMIFS(" + inner + ")", " ")
    refs = []
    for m in re.finditer(r"([+-]?)\s*([A-Z]{1,3})\$?(\d+)", s):
        col, row = m.group(2), int(m.group(3))
        if row == 1:
            continue
        refs.append((-1 if m.group(1) == "-" else 1, col, row))
    return refs


def parse_terms(formula, year_label, codes=None, inv=None, _depth=0):
    """Parse a formula into a list of (sign, conditions) terms.

    Each term is `(+1|-1, [(named_range, op, values), ...])`; the conditions are ANDed and a value
    list means OR within that condition (array constant). The year condition is normalised to the
    literal year label. Returns (terms, is_pure_sumifs).

    Composite formulas that add/subtract another code's cell (e.g. `SUMIFS(...) + C205`) are
    resolved recursively when `codes` (code -> info) and `inv` (row -> code) are supplied: the
    referenced cell's own terms are appended with the carried sign. Formulas with no SUMIFS and no
    resolvable ref -> ([], False).
    """
    if not formula:
        return [], False
    inners = _extract_sumifs(formula)
    if not inners:
        return [], False  # composite (e.g. C2-SUM(...), C4-C6) -- handled by composition
    terms = []
    for inner in inners:
        args = [a.strip() for a in _split_top_commas(inner)]
        # args[0] is the sum range ('executed'); the rest are (range, criteria) pairs
        conds = []
        rng = args[0]
        if rng != "executed":
            # some formulas put criteria range first when summing 'executed' implicitly; skip odd cases
            pass
        rest = args[1:]
        for i in range(0, len(rest) - 1, 2):
            named = rest[i].strip()
            crit = rest[i + 1].strip()
            if named == "year":
                conds.append(("year", "eq", [year_label]))
                continue
            if crit.startswith("{") and crit.endswith("}"):
                vals = [v.strip().strip('"') for v in _split_top_commas(crit[1:-1])]
                conds.append((named, "eq", vals))
            else:
                v = crit.strip().strip('"')
                if v.startswith("<>"):
                    conds.append((named, "ne", [v[2:]]))
                elif v.startswith("<="):
                    conds.append((named, "le", [v[2:]]))
                elif v.startswith(">="):
                    conds.append((named, "ge", [v[2:]]))
                elif v.startswith("<"):
                    conds.append((named, "lt", [v[1:]]))
                elif v.startswith(">"):
                    conds.append((named, "gt", [v[1:]]))
                else:
                    conds.append((named, "eq", [v]))
        terms.append((1, conds))   # every SUMIFS in this workbook is added (no negated SUMIFS)
    # Resolve top-level +CELL / -CELL composition by recursing into the referenced code's terms.
    if codes is not None and inv is not None and _depth < 5:
        for sign, _col, row in _residual_signed_refs(formula):
            ref_code = inv.get(row)
            if ref_code is None:
                continue
            ref_formula = codes[ref_code]["per_year"].get(year_label, {}).get("formula")
            ref_terms, ref_ok = parse_terms(ref_formula, year_label, codes, inv, _depth + 1)
            if ref_ok:
                terms.extend((sign * s, conds) for s, conds in ref_terms)
    return terms, True


def _crit_to_regex(value):
    """Excel wildcard ('*','?') -> anchored, case-insensitive regex."""
    out = []
    for ch in value:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.compile("^" + "".join(out) + "$", re.IGNORECASE)


def _cond_mask(df, named, op, values):
    """Boolean mask for one (range, op, values) condition. values is OR'd."""
    header = NAMED_RANGE_TO_HEADER.get(named)
    if header is None or header not in df.columns:
        # unknown range -> match nothing (will surface as a validation mismatch)
        return pd.Series(False, index=df.index)
    # Match Excel's text semantics LITERALLY: case-insensitive, but leading/trailing spaces are
    # significant on BOTH sides (Excel trims neither cells nor criteria). Some workbook criteria
    # AND cells carry trailing spaces -- e.g. func0 "10 Sustainable Urbanisation And Housing " and
    # the exclusion "<>0600 Unspecified " -- which must match the equally-spaced cells, while a
    # bare "0600 Unspecified" (no space) must NOT be excluded. Stripping either side breaks one of
    # these cases; literal matching reproduces 1214/1216 cells (the 2 left are a genuine Excel bug,
    # the EXCEL_FORMULA_ERRORS array-broadcast double-count -- see verification.md).
    col = df[header].astype("string")
    has_wild = any(("*" in v or "?" in v) for v in values)
    if op in ("eq", "ne"):
        if has_wild:
            m = pd.Series(False, index=df.index)
            for v in values:
                m = m | col.str.match(_crit_to_regex(v), na=False)
        else:
            low = col.str.casefold()
            wanted = {v.casefold() for v in values}
            m = low.isin(wanted)
        return ~m if op == "ne" else m
    # string comparisons (rare: econ3 "<264 ...")
    target = values[0].casefold()
    low = col.str.casefold()
    if op == "lt":
        return low.notna() & (low < target)
    if op == "gt":
        return low.notna() & (low > target)
    if op == "le":
        return low.notna() & (low <= target)
    if op == "ge":
        return low.notna() & (low >= target)
    return pd.Series(False, index=df.index)


def term_mask(df, term):
    m = pd.Series(True, index=df.index)
    for named, op, values in term:
        m = m & _cond_mask(df, named, op, values)
    return m


def code_value_and_mask(df_year, terms):
    """Excel-faithful value = sum over terms of Sum(executed in term). Mask = OR of term masks."""
    total = 0.0
    mask = pd.Series(False, index=df_year.index)
    exe = pd.to_numeric(df_year[NAMED_RANGE_TO_HEADER["executed"]], errors="coerce").fillna(0.0)
    for sign, conds in terms:
        tm = term_mask(df_year, conds)
        total += sign * float(exe[tm].sum())
        if sign > 0:                # subtractive terms (e.g. Total - debt) adjust the value only;
            mask = mask | tm        # they do not add rows to the membership mask
    return total, mask


# ----------------------------------------------------------------------------------------
# 4. Dimension membership (flat sets). EXP_CROSS_*/SBN are intersections/subnational -> excluded.
#    Composite members (social benefits, public order) are built by OR-ing leaf codes.
# ----------------------------------------------------------------------------------------
ECON_TOP = {  # display name -> leaf code(s) whose masks are OR'd
    "Wage bill": ["EXP_ECON_WAG_BIL_EXE"],
    "Capital expenditures": ["EXP_ECON_CAP_EXP_EXE"],
    "Goods and services": ["EXP_ECON_USE_GOO_SER_EXE"],
    "Subsidies": ["EXP_ECON_SUB_EXE"],
    "Other grants/transfers": ["EXP_ECON_OTH_GRA_EXE"],
    "Interest on debt": ["EXP_ECON_INT_DEB_EXE"],
    "Social benefits": ["EXP_ECON_SOC_ASS_EXE", "EXP_ECON_SOC_BEN_PEN_EXE"],
}
ECON_SUB = {
    "Allowances": ["EXP_ECON_ALL_EXE"],
    "Pension contributions": ["EXP_ECON_PEN_CON_EXE"],
    "Capital maintenance": ["EXP_ECON_CAP_MAI_EXE"],
    "Goods&svc basic services": ["EXP_ECON_GOO_SER_BAS_SER_EXE"],
    "Goods&svc employment contracts": ["EXP_ECON_GOO_SER_EMP_CON_EXE"],
    "Recurrent maintenance": ["EXP_ECON_REC_MAI_EXE"],
    "Social Assistance": ["EXP_ECON_SOC_ASS_EXE"],
    "Pensions": ["EXP_ECON_SOC_BEN_PEN_EXE"],
}
FUNC_TOP = {
    "Defense": ["EXP_FUNC_DEF_EXE"],
    "Public order and safety": ["EXP_FUNC_JUD_EXE", "EXP_FUNC_PUB_SAF_EXE"],
    "Economic affairs": ["EXP_FUNC_ECO_REL_EXE"],
    "Environmental protection": ["EXP_FUNC_ENV_PRO_EXE"],
    "Health": ["EXP_FUNC_HEA_EXE"],
    "Education": ["EXP_FUNC_EDU_EXE"],
    "Social protection": ["EXP_FUNC_SOC_PRO_EXE"],
    # Housing & community amenities (COFOG 706) = func0 "02 Lands.."/"10 Sustainable Urban.." composed
    # with Water&sanitation (EXP_FUNC_HOU_EXE = SUMIFS + the water&san cell). Water&sanitation itself
    # is a func_SUB leaf of this function, NOT a top-level func -- it lives in FUNC_SUB below.
    "Housing and community amenities": ["EXP_FUNC_HOU_EXE"],
}
FUNC_SUB = {
    "Agriculture": ["EXP_FUNC_AGR_EXE"],
    "Transport": ["EXP_FUNC_TRA_EXE"],
    "Roads": ["EXP_FUNC_ROA_EXE"],
    "Railroads": ["EXP_FUNC_RAI_EXE"],
    "Water transport": ["EXP_FUNC_WAT_TRA_EXE"],
    "Air transport": ["EXP_FUNC_AIR_TRA_EXE"],
    "Energy": ["EXP_FUNC_ENE_EXE"],
    "Energy (power)": ["EXP_FUNC_ENE_POW_EXE"],
    "Energy (oil & gas)": ["EXP_FUNC_ENE_OIL_EXE"],
    "Telecoms": ["EXP_FUNC_TEL_EXE"],
    "Water and sanitation": ["EXP_FUNC_WAT_SAN_EXE"],
    "Primary education": ["EXP_FUNC_PRI_EDU_EXE"],
    "Secondary education": ["EXP_FUNC_SEC_EDU_EXE"],
    "Tertiary education": ["EXP_FUNC_TER_EDU_EXE"],
    "Judiciary": ["EXP_FUNC_JUD_EXE"],
    "Public safety": ["EXP_FUNC_PUB_SAF_EXE"],
}
# Overlap detection runs on these dimensions only. `func_sub` (FUNC_SUB) is deliberately excluded
# pending expert sign-off on the func/func_sub COFOG hierarchy -- see the module docstring. FUNC_SUB
# is still defined above (used by the pipeline's func_sub tagging) but is not scanned for overlaps here.
DIMENSIONS = {"econ": ECON_TOP, "econ_sub": ECON_SUB, "func": FUNC_TOP}


# ----------------------------------------------------------------------------------------
# 4b. Known workbook formula errors (NOT parser bugs -- the python value is the correct one).
#     These cells fail validation because Excel's own SUMIFS is wrong, so the cached cell value
#     differs from the economically-correct disjoint sum this validator computes. Documented for
#     expert/Massimo sign-off and correction in verification.md. Keyed by code -> {years, note}.
# ----------------------------------------------------------------------------------------
EXCEL_FORMULA_ERRORS = {
    "EXP_CROSS_SBN_CAP_EXP_WAT_SAN_EXE": {
        "years": {"2022/23", "2023/24"},
        "note": ("Array-broadcast double-count. Formula is "
                 "SUM(SUMIFS(...,admin1,{\"districts\",\"Urban/Municipals\"},...,econ2,\"31*\") "
                 "+ SUM(SUMIFS(...,econ2,\"23 CONSUMPTION OF FIXED ASSETS\"))): the inner "
                 "SUM(...) collapses the 2nd term to a SCALAR which Excel then broadcasts across "
                 "the 2-element {districts,Urban/Municipals} array of the 1st term, adding the "
                 "'23 consumption' total twice. Excel cached is ~2x; the python value is correct. "
                 "Corrected formula wraps BOTH SUMIFS in their own SUM() before adding -- see "
                 "verification.md. (Same fragile shape: EXP_CROSS_SBN_CAP_EXP_ENE_EXE, "
                 "EXP_CROSS_SBN_CAP_EDU_EXE, EXP_CROSS_SBN_REC_EXP_ENE_EXE -- latent only because "
                 "their doubled term is ~0 in the validated years.)"),
    },
}


def excel_error_for(code, year):
    """Return the EXCEL_FORMULA_ERRORS note if (code, year) is a known workbook bug, else None."""
    rec = EXCEL_FORMULA_ERRORS.get(code)
    return rec["note"] if rec and year in rec["years"] else None


# ----------------------------------------------------------------------------------------
# 5. Main
# ----------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument("--workbook", default=os.path.join(here, "..", "temp", "Uganda BOOST.xlsx"))
    ap.add_argument("--microdata", default="/tmp/uganda_expenditure.csv")
    ap.add_argument("--out", default=os.path.join(here, "_detect"))
    ap.add_argument("--tol", type=float, default=0.005, help="relative tolerance for validation")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    print("Reading Executed-sheet formulas...")
    codes, year_label = read_executed_sheet(args.workbook)
    inv = {info["row"]: code for code, info in codes.items()}  # row -> code, for composite cell-refs
    years = [year_label[c] for c in YEAR_COLS]

    print(f"Loading microdata {args.microdata} ...")
    df = pd.read_csv(args.microdata, dtype=str, low_memory=False)
    df["__exe"] = pd.to_numeric(df[NAMED_RANGE_TO_HEADER["executed"]], errors="coerce").fillna(0.0)
    yr_col = NAMED_RANGE_TO_HEADER["year"]
    df = df[df[yr_col].isin(years)].copy()  # restrict to validatable Executed years
    by_year = {y: sub for y, sub in df.groupby(yr_col)}
    print(f"  {len(df):,} rows across {len(by_year)} validatable years")

    # ---- 5a. VALIDATION: per (code, year) parsed-formula sum vs Excel cached value ----
    val_rows = []
    for code, info in codes.items():
        for y in years:
            cell = info["per_year"].get(y, {})
            formula, cached = cell.get("formula"), cell.get("cached")
            terms, ok = parse_terms(formula, y, codes, inv)
            if not ok or cached is None or y not in by_year:
                continue
            py_val, _ = code_value_and_mask(by_year[y], terms)
            diff = py_val - cached
            rel = abs(diff) / cached if cached else (0.0 if abs(diff) < 1 else 1.0)
            match = rel <= args.tol or abs(diff) < 1
            excel_err = None if match else excel_error_for(code, y)
            val_rows.append({
                "code": code, "year": y, "excel": cached, "python": py_val,
                "diff": diff, "rel": rel, "match": match,
                # status: faithful reproduction / known Excel bug (python correct) / real parser miss
                "status": "match" if match else ("excel_formula_error" if excel_err
                                                 else "parser_mismatch"),
                "excel_error_note": excel_err or "",
            })
    val = pd.DataFrame(val_rows)
    val.to_csv(os.path.join(args.out, "validation.csv"), index=False)
    n_match = int(val["match"].sum()) if len(val) else 0
    n_xlerr = int((val["status"] == "excel_formula_error").sum()) if len(val) else 0
    n_parser = int((val["status"] == "parser_mismatch").sum()) if len(val) else 0
    print(f"Validation: {n_match}/{len(val)} cells reproduce Excel; {n_xlerr} known Excel formula "
          f"errors (python correct); {n_parser} unexplained parser mismatches")

    # ---- 5b. ONE-HOT for dimension members (year-aware) ----
    # For each dimension member, build a per-row mask using each row's year-variant formula.
    def member_mask(leaf_codes):
        mask = pd.Series(False, index=df.index)
        for y, sub in by_year.items():
            sub_mask = pd.Series(False, index=sub.index)
            for lc in leaf_codes:
                cell = codes.get(lc, {}).get("per_year", {}).get(y, {})
                terms, ok = parse_terms(cell.get("formula"), y, codes, inv)
                if ok:
                    _, m = code_value_and_mask(sub, terms)
                    sub_mask = sub_mask | m
            mask.loc[sub.index] = mask.loc[sub.index] | sub_mask
        return mask

    overlap_rows = []
    sample_cols = {}
    for dim, members in DIMENSIONS.items():
        masks = {name: member_mask(leaves) for name, leaves in members.items()}
        for name, m in masks.items():
            sample_cols[f"{dim}:{name}"] = m
        names = list(members)
        for a in range(len(names)):
            for b in range(a + 1, len(names)):
                na, nb = names[a], names[b]
                inter = masks[na] & masks[nb]
                cnt = int(inter.sum())
                if cnt == 0:
                    continue
                amt = float(df.loc[inter, "__exe"].sum())
                yrs = sorted(df.loc[inter, yr_col].unique().tolist())
                dim_total = float(df.loc[masks[na] | masks[nb], "__exe"].sum())
                overlap_rows.append({
                    "dimension": dim, "code_a": na, "code_b": nb,
                    "lines": cnt, "executed_overlap": amt,
                    "pct_of_pair": (amt / dim_total * 100) if dim_total else 0.0,
                    "years": ",".join(yrs),
                })
    overlaps = pd.DataFrame(overlap_rows).sort_values(
        ["dimension", "executed_overlap"], ascending=[True, False]
    )
    overlaps.to_csv(os.path.join(args.out, "overlaps.csv"), index=False)

    # one-hot sample for inspection
    sample = df[[yr_col, NAMED_RANGE_TO_HEADER["executed"]]].copy()
    for k, m in sample_cols.items():
        sample[k] = m.astype(int)
    sample.head(2000).to_csv(os.path.join(args.out, "onehot_sample.csv"), index=False)

    # ---- 5b2. WITHIN-formula self-double-count ----
    # A line matched by >1 ADDITIVE term of the SAME code's SUM(SUMIFS)+SUM(SUMIFS) is counted
    # multiple times inside that one category -- e.g. CapEx's `econ2 "31"/"23"` terms ALSO match its
    # `add,"capital"` term. This is NOT a cross-category overlap, so neither the dimension overlap
    # check (5b) nor the Excel-vs-python validation (5a) catches it: Excel and python both SUM the
    # terms the same (inflated) way, so they agree. `excess` = executed counted beyond the first =
    # the amount by which the code's own reported total is overstated.
    self_rows = []
    for code, info in codes.items():
        for y in years:
            terms, ok = parse_terms(info["per_year"].get(y, {}).get("formula"), y, codes, inv)
            add_terms = [conds for sign, conds in terms if sign > 0] if ok else []
            if len(add_terms) < 2 or y not in by_year:
                continue
            sub = by_year[y]
            n = None
            for conds in add_terms:
                tm = term_mask(sub, conds).astype(int)
                n = tm if n is None else n + tm
            dup = n >= 2
            if not dup.any():
                continue
            excess = float((sub.loc[dup, "__exe"] * (n[dup] - 1)).sum())
            self_rows.append({"code": code, "year": y, "lines": int(dup.sum()), "excess": excess})
    selfdup = pd.DataFrame(self_rows)
    selfdup.to_csv(os.path.join(args.out, "self_double.csv"), index=False)
    self_by_code = (selfdup.groupby("code")["excess"].sum().sort_values(ascending=False)
                    if len(selfdup) else pd.Series(dtype=float))
    print(f"Within-formula self-double-counting: {selfdup['code'].nunique()} codes, "
          f"Σ excess {selfdup['excess'].sum():,.0f}" if len(selfdup)
          else "Within-formula self-double-counting: none")

    # ---- 5c. Markdown report ----
    lines = ["# Uganda overcounting detection report", ""]
    lines.append(f"- Microdata rows (validatable years {years[0]}..{years[-1]}): **{len(df):,}**")
    lines.append(f"- Validation: **{n_match}/{len(val)}** (code, year) cells reproduce Excel within "
                 f"{args.tol:.1%}")
    if len(val):
        parser_bad = val[val["status"] == "parser_mismatch"].sort_values("rel", ascending=False)
        xl_bad = val[val["status"] == "excel_formula_error"].sort_values("rel", ascending=False)
        lines.append(f"- Parser correct on **{n_match + n_xlerr}/{len(val)}** cells "
                     f"({n_parser} unexplained mismatch(es), {n_xlerr} confirmed Excel formula "
                     f"error(s) where the python value is the correct one)")
        if len(parser_bad):
            lines += ["", "### Unexplained parser mismatches (fix parser/mapping first)", "",
                      "| code | year | excel | python | rel |", "|---|---|--:|--:|--:|"]
            for _, r in parser_bad.head(20).iterrows():
                lines.append(f"| {r.code} | {r.year} | {r.excel:,.0f} | {r.python:,.0f} | {r.rel:.1%} |")
        if len(xl_bad):
            lines += ["", "### Confirmed Excel formula errors (python value is correct)", "",
                      "| code | year | excel (wrong) | python (correct) | rel |",
                      "|---|---|--:|--:|--:|"]
            for _, r in xl_bad.iterrows():
                lines.append(f"| {r.code} | {r.year} | {r.excel:,.0f} | {r.python:,.0f} | {r.rel:.1%} |")
            lines += ["", "See verification.md for the corrected SUMIFS of each. Notes:"]
            for code, rec in EXCEL_FORMULA_ERRORS.items():
                lines.append(f"- **{code}** ({', '.join(sorted(rec['years']))}): {rec['note']}")
    if len(selfdup):
        lines += ["", "## Within-formula self-double-counting (a code's own SUMIFS terms overlap)", "",
                  "_A line matched by >1 additive term of the **same** code is counted twice inside its "
                  "own `SUM(SUMIFS)+SUM(SUMIFS)`. NOT a cross-category overlap, and invisible to "
                  "validation (Excel & python sum the terms the same way). `excess` = amount the code's "
                  "own total is overstated._", "", "| code | Σ excess | years |", "|---|--:|---|"]
        for code, exc in self_by_code.items():
            yrs = ",".join(sorted(selfdup[selfdup["code"] == code]["year"].unique()))
            lines.append(f"| {code} | {exc:,.0f} | {yrs} |")
    for dim in DIMENSIONS:
        sub = overlaps[overlaps["dimension"] == dim]
        lines += ["", f"## Overcounting within `{dim}` (flat, no hierarchy assumed)", ""]
        if sub.empty:
            lines.append("_No within-dimension overlaps detected._")
            continue
        lines += ["| code_a | code_b | lines | Σ executed overlap | years |",
                  "|---|---|--:|--:|---|"]
        for _, r in sub.iterrows():
            lines.append(f"| {r.code_a} | {r.code_b} | {r.lines:,} | {r.executed_overlap:,.0f} "
                         f"| {r.years} |")
    with open(os.path.join(args.out, "overcounting_report.md"), "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Wrote report + CSVs to {args.out}")


if __name__ == "__main__":
    main()
