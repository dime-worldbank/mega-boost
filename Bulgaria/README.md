# Bulgaria BOOST expenditures: Python rebuild

Two Databricks notebooks rebuild the Bulgaria BOOST expenditure dataset from the Ministry of Finance raw files in
`TXT/`, with no Stata and no intermediate files. Each writes CSV to `OUT_DIR` (`python_output/` locally).

| notebook | years | method | outputs |
|---|---|---|---|
| `BGR_extract_raw_microdata_txt_to_csv_2005_2019.py` | 2005-2019 | the BOOST team's Stata do-file rules (v1.4 to v1.9) | `BGR_expenditure+2005-2019.csv` |
| `BGR_extract_raw_microdata_txt_to_csv_2020_2024.py` | 2020-2024, one run | the newer BOOST team's yearly Excel workbooks ("YYYY BOOST update.xlsx") | `BGR_2020-2024.csv` |

Both follow the same sequence of cells: settings, imports, code lists, parse the extract(s), clean the expenditure
rows, parse the special-units report(s), label, write, check. The two methods differ in substance (see
"Why two methods"), so they are kept separate.

Run locally with `/opt/anaconda3/bin/python3 python-files/<notebook>.py` (pandas 3, numpy, xlrd, openpyxl). On
Databricks set `BASE` in the settings cell to the volume that holds `TXT/`.

## Inputs

Everything comes from `TXT/`; no workbook tab is read. Per year there are two files, the extract and the report, and
there are two auxiliary code lists, `labels_en.csv` (2005-2019) and `2020-2024 - legend.json` (2020-2024).

* **Annual extracts** `YYYY_annual_deatiled_data.txt` (older years: `Data_2005.txt`, `2006Q4-detailed data-bs.txt`,
  ...; 2022: `2022_annual_deatiled_data-final.txt`). Tab-delimited, CRLF, one row per unit, activity, financing and
  economic code with the adjusted and executed amounts: `quarter_id, para_type_id, ibsf_type_id, budget_unit_id,
  account_id, activity_id, act_type_id, OP_code_id, sub_para_id, adj_budget_amt, actual_amt`. Expenditure rows have
  `para_type_id` 2. The 2005 file has older column names and "z" as a code; the 2007 file has empty lines; the 2012
  file has two comma decimals. `TXT/2023_annual_deatiled_data(v1).txt` is the earlier version of the 2023 extract that
  the delivered 2023 data were built from; the notebook reads the newer file.
* **Consolidated Fiscal Program reports** `YYYY - special_spending_units.xls[x]`, one per year, in thousands of BGN:
  the defense-related "special spending units" whose spending is not in the extracts. Only the "EXPENDITURE BY
  FUNCTION" block is used. The 2005-2011, 2014 and 2015 reports were copied out of the BOOST team's v1.7 workbook
  sheets (the team's mapping columns removed); the 2018 report is not available, so 2018 has no special-unit rows.
  The 2021 report prints no paragraph codes; the notebook maps its line names to paragraphs with the table it
  derives from the other years' reports, where every name carries one and the same paragraph.
* **Code lists.** `python-files/labels_en.csv` (variable, code, label; from the do-files' `labels_en.do`) for
  2005-2019. `TXT/2020-2024 - legend.json` for 2020-2024: the union of the LEGEND tabs of the five yearly workbooks
  (economic codes with paragraph, sub-paragraph and expenditure type; financing codes; units with admin1-3;
  activities with func1-3; each entry lists the years whose tab carries it). The tabs never conflict except activity
  143, named in 2020-2022 and "n/a" in 2023-2024; the name is kept. The yearly `YYYY - legend.json` files are the
  individual tabs and are no longer read.

## Output columns

`year, admin1, admin2, admin3, func1, func2, func3, econ1, econ2, fin_source1, fin_source2, exp_type, transfer,
adjusted, executed`. Amounts in BGN. The 2005-2019 file contains codes; the 2020-2024 file carries labels ("0100
National Assembly").

## BGR_extract_raw_microdata_txt_to_csv_2005_2019.py (2005-2019)

Transcribed step for step from the v1.4 do-file (the code's cell titles keep that order) plus the rules the
v1.5-v1.7 do-files added; the v1.9 do-file only appends 2018 and 2019 to the same steps and reads its special units
from a hand-mapped workbook that is not available here.

1. **Code lists**: `labels_en.csv`.
2. **Parse the extracts**: one file per year; every column becomes a number, a value that does not parse becomes
   missing and is reported (as `destring, force` did); `year` from `quarter_id`.
3. **Clean the expenditure rows** (do-file lines 84-216): keep `para_type_id` 2; admin1 = 1 (central) unless the
   unit number exceeds 5100 and is not on the central list (5100, 5200, 5300, 5400, 6100, 6200, 8100, 9817, 9900,
   8200, 8300, 6300, 8400, 7100), = 3 for 5500, 5592, 5600, 5591; unit 2522 folded into 2500; fin_source1 from the
   activity type 1-3, else the ibsf type 4-9, ibsf 3 as 10; fin_source2 from the account code, overridden by a 98xxx
   programme code; func1 and func3 from the activity number; admin2 = 11 for central (22 for 1280 and 1780), 33 for
   social security, 44 for any non-social row financed from sources 4-9, 99 for 9999, the first two digits for
   municipalities; 2005 activity recodes; func2 from activity ranges; econ1 and econ2 from the sub-paragraph; rows
   with no amount dropped; 40.71 recoded 57.01; the 2011 privatisation fund; subtotal flags (a fixed list of
   paragraphs, plus 19.00 since v1.5); executed blanked on subtotals and adjusted blanked below paragraph level.
4. **Parse the special-units reports**: function from the roman-numeral header, function group from the sub-block
   header (wording changed over the years), economic code from the paragraph column taking the first code of a
   multi-code line, a line listing whole paragraphs ("01,02,...") as 11.00; amount from the "Incl. Special" column
   or, where a report lacks it, the sum of the special-unit columns; adjusted = executed; lines keyed on paragraph
   and amount because names drift by a row in old reports; from 2019 the whole-function block repeats its
   sub-blocks and is dropped when its total equals theirs; a paragraph total is a subtotal when its sub-lines are in
   the same block.
5. **Assemble**: rows and special units together; exp_type from the paragraph (1 personnel, 2 recurrent, 3 capital
   incl. 49.02, 4 other); transfer flag for paragraphs 30-32, 60-69, 74-78; unit 7900 is central with its own type 79
   (v1.7); sort; integer codes.
6. **Label**, 7. **Write**, 8. **Checks**: the yearly executed totals against the "TOTAL EXPENDITURE" of the fiscal
   program printed in the reports (equal to the leva in most years; 2009 -51m and 2014 +24m are in the published
   files too), and the special units against the rows the team mapped by hand for 2005-2017 (totals agree to about
   a million a year; 2016 +6m).

Deliberate departures from the do-files: the special units are parsed from the raw reports with one rule set instead
of taking the team's hand-mapped rows; rows with econ2 4902 get exp_type 3 as the do-files say (the shipped v1.4 file
gave them 2). Apart from the special units the result equals the shipped v1.7 file for 2005-2017 row for row.

## BGR_extract_raw_microdata_txt_to_csv_2020_2024.py (2020-2024)

The default rules are those of the 2023 and 2024 workbooks; 2020, 2021 and 2022 are special cases written as
`if YEAR == ...` where they apply, each reproducing a decision taken for that year's delivered data
(`Bulgaria BOOST 2015-2024 expenditure.xlsx`, sheet 2019-24).

1. **Code lists**: the merged legend, and the line-name to paragraph table read off the reports that print codes.
2. **Parse the extract**: unit padded to 4 characters; economic code written as "01.01"; the last three digits of
   the activity; ibsf, activity and programme types as integers; rows with an amount flagged.
3. **Clean the expenditure rows**. A paragraph total (XX.00) is the subtotal of the sub-paragraph rows under it and
   must not be counted twice; a paragraph reported without breakdown must be kept. Default: a total is dropped when
   the year's expenditure rows report sub-paragraphs of that paragraph anywhere (equal to "the legend lists
   sub-paragraphs" for 2023 and 2024). 2020-2022 use the workbooks' "next row" test instead, which drops a total
   when the next row starts with the same two digits, so the row order matters and each year's order is reproduced:
   2020 over every row type in the extract's order; 2021 sorted by unit (Excel put the 4-digit codes, numbers,
   before the padded "0100"-style codes, text), activity, activity type, ibsf type, programme, code, and 29.90
   left out; 2022 with the zero rows still present and the extract's leading block of municipalities moved to the
   end. Corrections: 2020 keeps the NHIF 39.00 totals the test dropped (appended at the bottom) and recodes unit
   5300's 40.71 as 57.01; 2022 keeps every dropped total whose unit and activity report no sub-paragraph of it and
   drops every 40.00 row.
4. **Parse the special-units report**: the lines with an amount in the "EXPENDITURE BY FUNCTION" block, each with
   its sub-block's function (B. SCIENCE is activity 161), its paragraph (2020: the first code of a multi-code line;
   later: the last) and the sum of the five special-unit columns (budget, National Fund, other EU funds, other
   international programmes, third parties); whole-function blocks dropped when their total equals their
   sub-blocks'; a line whose code the legend does not know (00-98) left out; 2021 also leaves out two lines of 13
   and 37 BGN that show as 0.0 thousand, and takes its paragraphs from the line-name table derived in cell 1.
5. **Label**: units, activities and economic codes from the legend; fin_source1 = the larger of the ibsf and
   activity types (2020: ibsf 3 read as source 10); fin_source2 = the programme code for EU funds, otherwise from the
   first character of the source 1 label; a blank expenditure type reads "0"; 2022 labels activity 143 "n/a".
   Special units: one unit (9999, "3 Other"; 2020 "1 Central"), state budget, the block's function; 2020 carries
   the expenditure type in econ1 and the paragraph label in econ2, 2021 the paragraph label in econ1 and a blank
   econ2, as the delivered data do.
6. **Assemble and write**: expenditure rows then special units (2020 the other way round); one CSV per year and one
   combined file.
7. **Check**: each year against the delivered rows, matched one to one on all 15 columns.

Revenue is not produced (the 2023 and 2024 workbooks build a revenue table; `BGR_2024_update.py` still has that
logic and reproduces the 2024 workbook's BOOST_Rev tab).

## Why two methods

The old method keeps every paragraph total but blanks its executed amount and keeps adjusted amounts only on
paragraph totals; the new method drops the totals whose sub-paragraphs exist and carries both amounts on every
sub-paragraph row. The old method derives the hierarchy by rules (including admin2 44 "EU funds" for centrally
financed rows with an EU source, about 1,900 rows a year); the new one reads it per code from the legend. Financing
sources, special-unit conventions and year fixes differ too. The two agree on admin1, func1 and func2 for the
2020-2024 codes except activities 279 and 845 and unit 8199.

## What the paragraph-total rules lose

The workbooks' next-row test compares a total only with the row below it. When the rows without an amount are removed
first, a total is often followed by the same paragraph's total of the next financing source or activity, and is
dropped although nothing under it exists: in 2022, 187 rows of 45.00 and 227 of 51.00, and the eight NHIF 39.00
rows every year. Sorting the rows helps (2021) but does not fix single-code activities such as NHIF. The legend rule
avoids all that but drops every 40.00 total wherever the legend lists 40.71 (2021 on): 114-155 million BGN of
scholarships a year. A test that drops a total only when its own unit and activity report a sub-paragraph loses
nothing in any year; it is not applied, so that each year reproduces its delivered data.

## Verification against the delivered files

Row for row against `Bulgaria BOOST 2015-2024 expenditure.xlsx` (sheet 2019-24):

| year | ours | file | identical | difference |
|---|---|---|---|---|
| 2020 | 151,017 | 151,018 | 151,017 | the file's duplicated NHIF row (109.7m) |
| 2021 | 159,509 | 159,509 | 159,509 | none |
| 2022 | 160,459 | 161,059 | 160,459 | the file's 600 zero-amount special-unit lines |
| 2023 | 161,391 | 161,978 | 161,072 | the file's special units are the 2022 block (1,140.9m); 137 keys from the earlier extract version (net 0.99m); 606 zero lines; activity 143 named |
| 2024 | 162,626 | 162,626 | 162,605 | activity 143 named on 21 rows |

Against `Bulgaria BOOST reduced.xlsx` (Expenditure sheet, aggregated to its columns), executed in million BGN:

| year | ours | file | difference | cause |
|---|---|---|---|---|
| 2006-2013 | | | 0.0 | identical to the leva (two special-unit lines coded by the last instead of the first code in 2006-2011, no effect on totals) |
| 2014 | 32,506.3 | 32,545.6 | -39.2 | 19.01 sign flip |
| 2015 | 34,684.6 | 35,492.3 | -807.7 | 19.01 sign flip -809.0; special units +1.3 |
| 2016 | 32,493.9 | 32,634.7 | -140.8 | 19.01 sign flip -147.0; special units +6.4 |
| 2017 | 34,471.1 | 34,523.7 | -52.6 | 19.01 sign flip |
| 2018 | 36,037.2 | 39,623.7 | -3,586.5 | special units missing on our side -3,478.7; 19.01 sign flip -107.9 |
| 2019 | 45,201.0 | 45,127.1 | +73.9 | file booked special units from the budget column only (+145.5); 19.01 sign flip -71.6 |
| 2020 | 47,747.7 | 47,857.4 | -109.7 | the file's duplicated NHIF row |
| 2021-2024 | | | 0.0 | identical in amounts (2022, 2023: the file's zero lines and, in 2023, its incremented "N State budget" special-unit labels) |

19.01 "paid taxes, charges and administrative sanctions" carries negative amounts in the extracts; the reduced file
turned them positive (its NOTE sheet), so the difference is twice the negatives.

## Known facts about the delivered files

* The delivered 2020 data are the 2020 workbook's boost tab with the NHIF row pasted twice; 2021 is the workbook's
  boost tab; 2022 is the workbook plus four hand edits (totals without breakdown restored, 29.90 kept, 40.00
  deleted, 143 "n/a"), built from the final extract; 2023 pastes the 2022 special-unit block and uses the v1
  extract; 2024 is the workbook's BOOST tab.
* The 2023 workbook's special-unit tab has "1 State budget" ... "176 State budget" on its last 176 rows (Excel's
  fill handle); not reproduced.
* The 2020 special-unit econ1/econ2 and the 2021 econ2 columns are shifted in the workbooks and the delivered file;
  reproduced as delivered.
* All special-unit rows are labelled "0 State budget" although 2-3 % of their amounts are EU or other international
  money (the five report columns).

## Open items

* The 2018 special-units report (or the team's "Special_spending_units_2005-2019.xls" that the v1.9 do-file reads).
* Placeholder labels: `labels_en.csv` has "(name to confirm)" for units 2028, 2029, 2233, 2234, 7400, 7500, 8199,
  activities 222, 279, 448, 625, 761, 845, 867 and sub-paragraphs 28.20, 28.90; the merged legend has "n/a" for
  05.58, 28.10-28.90, 33.07, 40.71, programmes 98121, 98222, 98321-98324, 99001, units 2028, 7400, 7500, 8199 and
  activities 142, 222, 279, 433, 625, 748, 802, 845, 880. Each file names some codes the other leaves blank.
* Programme codes 98122 and 98223 have no legend entry (financing source 2 shows #N/A).

## Other files

`BGR_2020_update.py` ... `BGR_2024_update.py`: the yearly scripts the combined notebook replaced (kept as the
year-by-year record; `BGR_2024_update.py` also builds the 2024 revenue table). `compare_with_dta.py` and
`compare_with_reference.py`: comparison tools against a Stata file and against a labelled workbook.
