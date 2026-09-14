# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import re

import numpy as np
import pandas as pd

COUNTRY = 'Bulgaria'
OUT_DIR = prepare_microdata_csv_dir(COUNTRY)
BASE = Path(f"{RAW_INPUT_DIR}/Bulgaria")
LABELS_PATH = BASE / "labels_en.csv" #

# COMMAND ----------

# 1. Raw extracts (do-file lines 22-78).  One tab-delimited file per year, CRLF line endings.
#    Fields may be blank or padded; blank means missing.  Every column is a number (the codes are used
#    in arithmetic); a value that does not parse becomes missing and is reported, as `destring, force`
#    did in the do-files (two comma decimals in the 2012 amounts).
RAW_FILES = [
    ("Data_2005.txt", 2005),
    ("2006Q4-detailed data-bs.txt", 2006),
    ("2007Q4-detailed data-bs.txt", 2007),
    ("2008Q4_detailed_data.txt", 2008),
    ("Data_2009.txt", 2009),
    ("2010Q4-detailed data.txt", 2010),
    ("2011_annual-detailed data.txt", 2011),
    ("2012_annual-detailed data.txt", 2012),
    ("2013_annual-detailed data.txt", 2013),
    ("2014_annual_detailed_data.txt", 2014),
    ("2015_annual_deatiled_data.txt", 2015),
    ("2016_annual_deatiled_data.txt", 2016),
    ("2017_annual_deatiled_data.txt", 2017),
    ("2018_annual_deatiled_data.txt", 2018),
    ("2019_annual_deatiled_data.txt", 2019),
]

yearly = []
for fname, year in RAW_FILES:
    df = pd.read_csv(BASE / "TXT" / fname, sep="\t", dtype=str, keep_default_na=False,
                     skip_blank_lines=False, encoding="latin-1")
    df.columns = [c.strip().lower() for c in df.columns]
    if year == 2005:  # lines 36-40: older column names, and "z" as a code
        df = df.rename(columns={"ibsf_typ_id": "ibsf_type_id", "vedomstvo_id": "budget_unit_id",
                                "budget_amt": "adj_budget_amt"})
        df["ibsf_type_id"] = df["ibsf_type_id"].str.strip().replace({"z": "7"})
    for c in df.columns:
        col = df[c].str.strip()
        col = col.mask(col == "", None)
        values = pd.to_numeric(col, errors="coerce")
        bad = col[values.isna() & col.notna()]
        if len(bad):
            print(f"   {fname}: {len(bad)} unparsable {c} value(s) set to missing: {sorted(bad.unique())[:5]}")
        df[c] = values.astype("float64")
    df["year"] = np.trunc(df["quarter_id"] / 10)  # line 31
    df = df.drop(columns="quarter_id")
    assert df["year"].dropna().eq(year).all(), f"{fname}: unexpected year values"
    print(f"{fname:36s} {len(df):>9,} rows, {df.shape[1]} columns")
    yearly.append(df)

data = pd.concat(yearly, ignore_index=True)[  # lines 65-77
    ["year", "para_type_id", "ibsf_type_id", "account_id", "act_type_id", "op_code_id",
     "budget_unit_id", "activity_id", "sub_para_id", "adj_budget_amt", "actual_amt"]]
del yearly
print(f"appended: {len(data):,} rows")

# COMMAND ----------

# 2. Expenditures and BOOST classification (lines 84-216)
df = data[data["para_type_id"] == 2].drop(columns="para_type_id").copy()  # lines 86-87
del data
print(f"expenditures only (para_type_id == 2): {len(df):,} rows")
bu = df["budget_unit_id"]

# admin1 -- lines 98-103 (v1.5 added 7100 to the central units and 5591 to social security)
df["admin1"] = 1.0
df.loc[(bu > 5100) & bu.notna(), "admin1"] = 2
df.loc[bu.isin([5100, 5200, 5300, 5400, 6100, 6200, 8100, 9817, 9900, 8200, 8300, 6300, 8400, 7100]), "admin1"] = 1
df.loc[bu.isin([5500, 5592, 5600, 5591]), "admin1"] = 3

# line 106: Sport Lottery flows -> Ministry of Youth and Sport
df.loc[bu == 2522, "budget_unit_id"] = 2500
bu = df["budget_unit_id"]

# fin_source1 -- lines 108-113
act, ibsf = df["act_type_id"], df["ibsf_type_id"]
df["fin_source1"] = np.where((act == 0) & (ibsf == 0), 0.0, np.nan)
df.loc[act.between(1, 3), "fin_source1"] = act
df.loc[ibsf.between(4, 9), "fin_source1"] = ibsf
df.loc[ibsf == 3, "fin_source1"] = 10

# fin_source2 -- lines 115-117
acc, op = df["account_id"], df["op_code_id"]
df["fin_source2"] = np.where((act == 0) & (ibsf == 0) & (acc == 0), 0.0, np.nan)
df.loc[acc.notna(), "fin_source2"] = acc
df.loc[np.trunc(op / 1000) == 98, "fin_source2"] = op

# func1 / func3 -- lines 119-120
df["func1"] = np.trunc(df["activity_id"] / 1000)
df["func3"] = df["activity_id"] % 1000

# admin2 -- lines 122-127
a1, fs1 = df["admin1"], df["fin_source1"]
df["admin2"] = np.where(a1 == 1, 11.0, np.nan)
df.loc[(a1 == 1) & bu.isin([1280, 1780]), "admin2"] = 22
df.loc[a1 == 3, "admin2"] = 33
df.loc[(a1 != 3) & fs1.between(4, 9), "admin2"] = 44
df.loc[bu == 9999, "admin2"] = 99
df.loc[a1 == 2, "admin2"] = np.trunc(bu / 100)

# func3 recodes for 2005 -- lines 132-144, applied one after another as in the do-file
is2005 = df["year"] == 2005
for old, new in [(721, 861), (722, 862), (723, 863), (728, 864), (729, 865),
                 (208, 218), (207, 282), (874, 284),
                 (863, 875), (864, 876), (865, 877)]:
    df.loc[(df["func3"] == old) & is2005, "func3"] = new

# func2 -- lines 146-173
f3 = df["func3"]
df["func2"] = np.nan
for lo, hi, code in [(101, 139, 11), (141, 158, 12), (161, 179, 13), (201, 219, 21),
                     (221, 239, 22), (241, 259, 23), (261, 279, 24), (281, 289, 25),
                     (301, 389, 31), (401, 469, 41), (501, 501, 51), (511, 519, 52),
                     (521, 589, 53), (601, 619, 61), (621, 629, 62), (701, 708, 71),
                     (711, 719, 72), (731, 759, 73), (761, 768, 74), (801, 809, 81),
                     (811, 829, 82), (831, 849, 83), (851, 859, 84), (861, 865, 85),
                     (866, 898, 86), (910, 998, 91)]:
    df.loc[f3.between(lo, hi), "func2"] = code

# line 176: align func1 with func3
f1_from_f3 = np.trunc(f3 / 100)
fix = f3.notna() & (df["func1"] != f1_from_f3) & (f1_from_f3 != 0)
print(f"func1 realigned with func3 on {int(fix.sum()):,} rows")
df.loc[fix, "func1"] = f1_from_f3

# lines 178-183
df["econ1"] = np.trunc(df["sub_para_id"] / 100)
df = df.rename(columns={"sub_para_id": "econ2", "budget_unit_id": "admin3",
                        "adj_budget_amt": "adjusted", "actual_amt": "executed"})

# lines 187-188
df = df.drop(columns=["ibsf_type_id", "account_id", "act_type_id", "op_code_id", "activity_id"])
df = df[~((df["adjusted"].isna() | (df["adjusted"] == 0)) & (df["executed"].isna() | (df["executed"] == 0)))]
print(f"after dropping zero/missing rows: {len(df):,} rows")

# lines 192-194: state reserve transactions
reserve = df["econ2"] == 4071
print(f"state reserve (econ2 == 4071) recoded on {int(reserve.sum()):,} rows")
df.loc[reserve, "econ1"] = 57
df.loc[reserve, "econ2"] = 5701

# line 198: 2011 privatization fund
df.loc[(df["fin_source2"] == 2019) & (df["year"] == 2011), "fin_source2"] = 19

# subtotal flags -- lines 204-210, plus the v1.5 rule for paid taxes (econ2 1900)
e1, e2, yr, fs1 = df["econ1"], df["econ2"], df["year"], df["fin_source1"]
top = e1 * 100 == e2
df["subtotal"] = 0.0
df.loc[e1.isin([1, 2, 10, 21, 22, 29, 33, 42, 43, 49, 57]) & top, "subtotal"] = 1
df.loc[(e1 == 5) & (yr != 2005) & top, "subtotal"] = 1
df.loc[e1.isin([25, 26, 28, 39, 41]) & (yr == 2005) & top, "subtotal"] = 1
df.loc[e1.isin([52, 53, 55]) & ~((yr == 2005) & fs1.isin([1, 2, 3])) & top, "subtotal"] = 1
df.loc[e2 == 1900, "subtotal"] = 1

# lines 213-216
df.loc[df["subtotal"] != 0, "executed"] = np.nan
df.loc[e2.notna() & (e1 * 100 != e2), "adjusted"] = np.nan  # adjusted kept at econ1 level only
df = df[~((df["adjusted"].isna() | (df["adjusted"] == 0)) & (df["executed"].isna() | (df["executed"] == 0)))]
df = df.reset_index(drop=True)
print(f"classified expenditures: {len(df):,} rows")

# COMMAND ----------

!pip install xlrd

# COMMAND ----------

# 3. Special spending units, mapped from the Ministry of Finance "Consolidated Fiscal Program" reports
#    (one per year, in thousands of BGN).  Only the "EXPENDITURE BY FUNCTION" part is used:
#    * the function (func1) comes from the roman-numeral header, the function group (func2) from
#      the sub-block header, whose wording changed over the years (see FUNC2_BY_HEADER);
#    * the economic code comes from the paragraph column, first code when a line lists several;
#      a line listing whole paragraphs ("01,02,03,04,05,07,08,10") is the aggregate current
#      expenditure of the older defense and security blocks and becomes paragraph 11.00;
#    * the amount is the "Incl. Special spending units" column, or, when a report lacks it, the sum
#      of the special-unit sub-columns; both are in thousands of BGN.  Adjusted and executed hold
#      the same amount, as in the rows the BOOST team mapped by hand for 2005-2017;
#    * from 2019 the reports put a breakdown of the whole function before its sub-blocks; that
#      block repeats the sub-blocks and is dropped when its total equals theirs;
#    * a paragraph total (XX-00) is a subtotal when its sub-lines are in the same block, and the
#      11.00 aggregate is a subtotal when the block also has the detailed wage lines.
#    In the older reports the line names drift by a row inside a block while the paragraph codes
#    and amounts stay aligned, so lines are keyed on the paragraph column.
SSU_REPORTS = {
    2005: "2005 - special_spending_units.xlsx", 2006: "2006 - special_spending_units.xlsx",
    2007: "2007 - special_spending_units.xlsx", 2008: "2008 - special_spending_units.xlsx",
    2009: "2009 - special_spending_units.xlsx", 2010: "2010 - special_spending_units.xlsx",
    2011: "2011 - special_spending_units.xlsx",
    2012: "2012 - special_spending_units_28.05.14.xls", 2013: "2013 - special_spending_units_29.05.14.xls",
    2014: "2014 - special_spending_units.xlsx", 2015: "2015 - special_spending_units.xlsx",
    2016: "2016 - special_spending_units.xls", 2017: "2017 - special_spending_units.xls",
    2018: "2018 - special_spending_units.xls", 2019: "2019 - special_spending_units.xls",
}
FUNC2_BY_HEADER = [  # regex on the sub-block header -> func2 (the wording changed over the years)
    (r"EXECUTIVE", 11), (r"GENERAL SERVICES|COMMON SERVICES", 12), (r"SCIENCE", 13),
    (r"^A\.?\s*DEFEN", 21), (r"POLICE|SAFETY", 22), (r"JUDICIAL|JURISDICTION|JURIDICAL", 23), (r"PRIS", 24),
    (r"CIVIL PROTECTION", 25),
    (r"PENSIONS", 51), (r"EMPLOYMENT|SOCIAL WELFARE", 53), (r"SOCIAL ASSISTANCE", 52),
    (r"^A\.?\s*HOUSING", 61), (r"ENVIRONMENT", 62),
    (r"RECREATION", 71), (r"PHYSICAL|\bSPORT", 72), (r"\bCULTURE", 73), (r"RELIGIO", 74),
    (r"MINING", 81), (r"AGRICULTURE", 82), (r"TRANSPORT", 83), (r"INDUSTRY|MANUFACTURING", 84),
    (r"TOURISM", 85), (r"OTHER ECONOMIC", 86),
]
ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9}
CYRILLIC_LOOKALIKES = str.maketrans("ІХ", "IX")  # some sheets write "IX." with Cyrillic letters
is_text = lambda v: isinstance(v, str)
NUMBER = re.compile(r"^-?\d+(\.\d+)?$")

reports, fiscal_program_total = {}, {}
for year, fname in SSU_REPORTS.items():
    path = BASE / "TXT" / fname
    if not path.exists():
        print(f"WARNING: {fname} not found; {year} has no special spending units")
        continue
    sheet = (pd.read_csv(path, header=None, dtype=str, keep_default_na=False) if fname.endswith(".csv")
             else pd.read_excel(path, sheet_name=0, header=None))
    # every cell becomes text (whitespace collapsed), a number, or None
    sheet = sheet.applymap(lambda v: None if v is None or (isinstance(v, float) and np.isnan(v))
                      else (float(v) if not is_text(v) else
                            (None if not v.strip() else
                             (float(v.strip()) if NUMBER.match(v.strip()) else re.sub(r"\s+", " ", v).strip()))))
    sheet.columns = range(sheet.shape[1])
    text = sheet.applymap(lambda v: v if is_text(v) else "")
    first_data = text.index[text.apply(lambda col: col.str.contains("REVENUE", na=False)).any(axis=1)][0]
    header = {j: " / ".join(v for v in text.loc[:first_data - 1, j] if v) for j in sheet.columns}
    name_col = next(j for j in sheet.columns if text[j].map(lambda v: "EXPENDITURE BY FUNCTION" in v).any())
    para_counts = {j: int(text[j].str.match(r"^\d{2}-\d{2}").sum()) for j in sheet.columns}
    para_col = max(para_counts, key=para_counts.get)
    if para_counts[para_col] <= 50:
        para_col = None
    incl = [j for j, h in header.items() if "Incl. Special" in h or ("OF WHICH" in h and "Special" in h)]
    amount_cols = incl[:1] or [j for j, h in header.items() if "SSU" in h or ("Special" in h and "Budget" in h)]
    cfp = [j for j, h in header.items() if "Consolidated" in h and "Incl" not in h]  # whole fiscal program
    total_row = text.index[text.apply(lambda row: row.str.startswith("TOTAL EXPENDITURE")).any(axis=1)]
    if cfp and len(total_row):
        fiscal_program_total[year] = pd.to_numeric(sheet.loc[total_row[0], cfp[0]], errors="coerce") * 1000
    reports[year] = pd.DataFrame({
        "name": sheet[name_col],
        "para": sheet[para_col] if para_col is not None else None,
        "amount": sum(pd.to_numeric(sheet[j], errors="coerce").fillna(0.0) for j in amount_cols),
    })
    print(f"{fname:44s} {len(sheet):>5} rows; paragraph column {'yes' if para_col is not None else 'NO '}; "
          f"amount from {[header[j][:40] for j in amount_cols]}")

rows = []
for year, r in reports.items():
    start = r.index[r["name"].map(lambda v: is_text(v) and "EXPENDITURE BY FUNCTION" in v)][0]
    func1 = func2 = None
    blocks = []  # one entry per block: its function codes, its "Total expenditure" line and its lines
    for _, x in r.loc[start + 1:].iterrows():
        nm = x["name"] if is_text(x["name"]) else ""  # a drifted name leaves some lines nameless
        roman = re.match(r"^([IVX]+)\.", nm.translate(CYRILLIC_LOOKALIKES))
        if roman and nm == nm.upper() and roman.group(1) in ROMAN:  # function header, e.g. "III. EDUCATION"
            func1 = ROMAN[roman.group(1)]
            func2 = func1 * 10 + 1
            blocks.append({"level": "function", "func1": func1, "func2": func2, "total": None, "lines": []})
            continue
        if nm and nm == nm.upper() and (re.match(r"^[A-E]\.?\s+[A-Z]", nm) or nm.startswith("CIVIL PROTECTION")):
            func2 = next(f2 for pattern, f2 in FUNC2_BY_HEADER if re.search(pattern, nm))  # sub-block header
            blocks.append({"level": "sub", "func1": func1, "func2": func2, "total": None, "lines": []})
            continue
        if re.match(r"^Total expen", nm) and blocks:
            blocks[-1]["total"] = round(x["amount"] * 1000, 3)
            continue
        para = x["para"]
        if is_text(para) and re.match(r"^\d{2}-\d{2}", para):
            econ2 = int(para[:2]) * 100 + int(para[3:5])
        elif is_text(para) and re.match(r"^\d{2}(\s*,\s*\d{2})+$", para):
            econ2 = 1100  # aggregate current expenditure of the older defense and security blocks
        else:
            continue  # aggregate line without a paragraph (Current expenditure, Capital expenditure, ...)
        blocks[-1]["lines"].append((year, func1, func2, econ2, round(x["amount"] * 1000, 3)))
    for i, b in enumerate(blocks):
        if b["level"] == "function":
            subs = []
            for nxt in blocks[i + 1:]:
                if nxt["level"] == "function":
                    break
                subs.append(nxt)
            if subs and b["total"] is not None and abs(b["total"] - sum(s["total"] or 0 for s in subs)) < 1:
                continue  # the whole-function breakdown repeats its sub-blocks
        rows.extend(b["lines"])

special = pd.DataFrame(rows, columns=["year", "func1", "func2", "econ2", "adjusted"]).astype("float64")
special["executed"] = special["adjusted"]
special["econ1"] = np.trunc(special["econ2"] / 100)
special["admin1"], special["admin2"], special["admin3"] = 1.0, 99.0, 9999.0
special["func3"], special["fin_source1"], special["fin_source2"], special["subtotal"] = np.nan, 0.0, 0.0, 0.0
has_sub = special[special["econ2"] % 100 != 0].groupby(["year", "func2"])["econ1"].agg(set)
for (y, f2), paragraphs in has_sub.items():
    blk = (special["year"] == y) & (special["func2"] == f2)
    special.loc[blk & (special["econ2"] % 100 == 0) & special["econ1"].isin(paragraphs), "subtotal"] = 1
detailed = special[special["econ1"].isin([1, 2, 5, 8, 10])].groupby(["year", "func2"]).size()
for (y, f2) in detailed.index:
    special.loc[(special["year"] == y) & (special["func2"] == f2) & (special["econ2"] == 1100), "subtotal"] = 1
print(f"special spending units mapped: {len(special):,} lines, {int(special['subtotal'].sum())} subtotals")

# cleaning (v1.4 do-file lines 226-227 and the v1.5 rule that drops subtotals)
special = special[~((special["adjusted"] == 0) & (special["executed"] == 0))].copy()
special["func3"] = special["func3"].fillna(special["func2"])
special = special[special["subtotal"] != 1]
print("special spending units cleaned, executed by year (million BGN):",
      (special.groupby("year")["executed"].sum() / 1e6).round(1).to_dict())

# COMMAND ----------

# 4. Final database (lines 238-302)
df = pd.concat([df, special.reindex(columns=df.columns)], ignore_index=True)  # line 239
e1, e2 = df["econ1"], df["econ2"]

# exp_type -- lines 243-250
df["exp_type"] = np.where(e1.isin([1, 2, 3, 4, 5, 7]), 1.0, np.nan)
df.loc[e1.isin([6, 8, 10, 11, 21, 22, 25, 26, 27, 28, 29, 33, 39, 40, 41, 42, 43, 44,
                45, 46, 49, 19]), "exp_type"] = 2
df.loc[(e2 == 4902) | e1.isin([51, 52, 53, 54, 55, 57]), "exp_type"] = 3
df.loc[df["exp_type"].isna(), "exp_type"] = 4
# transfer -- line 251
df["transfer"] = (e1.between(30, 32) | e1.between(60, 69) | e1.between(74, 78)).astype("float64")

# line 292: totals by year
print(df.groupby("year")[["executed", "adjusted"]].sum().to_string(float_format=lambda v: f"{v:,.0f}"))

# v1.7 (2017): the Ministry for the EU Presidency, unit 7900, is central but keeps its own unit type 79
df.loc[df["admin2"] == 79, "admin1"] = 1

# lines 293-300: drop subtotal, order, sort, keep
columns = ["year", "admin1", "admin2", "admin3", "func1", "func2", "func3", "econ1", "econ2",
           "fin_source1", "fin_source2", "exp_type", "transfer", "adjusted", "executed"]
df = df[columns].sort_values(columns, na_position="last", kind="mergesort").reset_index(drop=True)
print(f"final database: {len(df):,} rows, {df.shape[1]} columns")


# COMMAND ----------

labels = pd.read_csv(LABELS_PATH, keep_default_na=False, encoding="utf-8")
labelled = df.copy()
for var, g in labels.groupby("variable", sort=False):
    labelled[var] = df[var].map(dict(zip(g["code"], g["label"])))
    unlabelled = sorted(df.loc[labelled[var].isna() & df[var].notna(), var].unique())
#     assert not unlabelled, f"{var}: no label for codes {unlabelled}"
# labelled_path = OUT_DIR / "Bulgaria BOOST v1.4_expenditures 2005-2014 (en).csv"
# labelled.to_csv(labelled_path, index=False, float_format="%.17g", lineterminator="\n", encoding="utf-8")
# print(f"wrote {labelled_path}")

# COMMAND ----------

with pd.option_context('display.max_rows', None):
    display(labelled.groupby(['func1','year'])['executed'].sum().to_frame())

# COMMAND ----------


