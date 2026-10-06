# Bulgaria BOOST expenditures: Python rebuild

Bulgaria's pipeline runs end to end from the Ministry of Finance raw files, in the pattern of Albania's and Colombia's:
two Databricks notebooks rebuild the BOOST expenditure microdata, with no Stata and no intermediate files, and write
one CSV per year, `YYYY.csv`, to the country's `raw_microdata_csv` folder; the DLT pipeline starts from those files.

| file | years | method | output |
|---|---|---|---|
| `BGR_extract_raw_microdata_txt_to_csv_2005_2019.py` | 2005-2019 | the BOOST team's Stata do-file rules (v1.4 to v1.9) | `YYYY.csv`, one per year |
| `BGR_extract_raw_microdata_txt_to_csv_2020_onward.py` | 2020 onward, every year whose extract is in `TXT/` | the newer BOOST team's yearly Excel workbooks ("YYYY BOOST update.xlsx") | `YYYY.csv`, one per year |
| `BGR_transform_load_raw_dlt.py` | every `YYYY.csv` | the BOOST workbook's `Executed` sheet criteria, applied to the labels | `bgr_boost_bronze`, `bgr_boost_silver`, `bgr_boost_gold` |

Both notebooks follow the same sequence of cells: settings, code list, parse the extract(s), clean the expenditure
rows, parse the special-units report(s), label, write, check. The two methods differ in substance (see "Why two
methods"), so they are kept separate.

## Paths and running

Both notebooks take their paths from `../utils` like the other extract notebooks: inputs under
`RAW_INPUT_DIR/Bulgaria/` (`TXT/`, `labels_en.json`, and the delivered workbook
`Bulgaria BOOST 2015-2024 expenditure.xlsx` for the 2020-onward check cell) and outputs in `raw_microdata_csv/Bulgaria`
(`prepare_raw_microdata_csv_dir`, whose string result is wrapped in `Path`). Each starts with a `!pip install xlrd`
cell, since the reports are `.xls` files, and defines `applymap`, which calls `DataFrame.applymap` where pandas has
it and `DataFrame.map` on pandas 3. To run a notebook locally (pandas 3, numpy, xlrd, openpyxl), exec the file with
`Path`, `RAW_INPUT_DIR` and `prepare_raw_microdata_csv_dir` defined for local folders and the `!pip` line removed.

## Adding a year

1. Put the Ministry of Finance extract `YYYY_annual_deatiled_data.txt` and the Consolidated Fiscal Program report
   `YYYY - special_spending_units.xls` in `Data from authorities/Bulgaria/TXT/` on the volume. The yearly
   "BOOST update.xlsx" workbook is not needed; if its LEGEND tab names codes `labels_en.json` lacks, add them there.
2. Run `BGR_extract_raw_microdata_txt_to_csv_2020_onward.py`: it processes every year it finds and writes
   `raw_microdata_csv/Bulgaria/YYYY.csv`. A code the list does not know reads "#N/A" in the output.
3. Run the pipeline; its bronze table reads every `YYYY.csv`.

## Inputs

Everything comes from `TXT/` and one code list; no workbook tab is read. Per year there are two files, the extract
and the report.

* **Annual extracts** `YYYY_annual_deatiled_data.txt` (older years: `Data_2005.txt`, `2006Q4-detailed data-bs.txt`,
  ...; 2022: `2022_annual_deatiled_data-final.txt`). Tab-delimited, CRLF, one row per unit, activity, financing and
  economic code with the adjusted and executed amounts: `quarter_id, para_type_id, ibsf_type_id, budget_unit_id,
  account_id, activity_id, act_type_id, OP_code_id, sub_para_id, adj_budget_amt, actual_amt`. Expenditure rows have
  `para_type_id` 2. Negative amounts (refunds, mostly paragraph 19) are written with a leading minus and kept as
  such. The 2005 file has older column names and "z" as a code; the 2007 file has empty lines; the 2012 file has two
  comma decimals. `TXT/2023_annual_deatiled_data(v1).txt` is the earlier version of the 2023 extract that the
  delivered 2023 data were built from; the notebook reads the newer file.
* **Consolidated Fiscal Program reports** `YYYY - special_spending_units.xls[x]`, one per year, in thousands of BGN:
  the defense-related "special spending units" whose spending is not in the extracts. Only the "EXPENDITURE BY
  FUNCTION" block is used. The 2005-2011, 2014 and 2015 reports were copied out of the BOOST team's v1.7 workbook
  sheets (the team's mapping columns removed); the 2018 report is the 2018 sheet of the team's
  `Special_spending_units_2005-2018.xls` in the same way (its first eleven columns, laid out like the 2019 report).
  The 2021 report prints no paragraph codes; the notebook maps its line names to paragraphs with the table it
  derives from the other years' reports, where every name carries one and the same paragraph.
* **Code list** `labels_en.json`, kept on the volume next to `TXT/` (it is not in this repository): every unit with
  its admin1-3 labels, every activity with func1-3, every economic code with econ1-2 and expenditure type, every
  financing source with its fin_source1 label and the fin_source2 the workbooks attach to it, every account and
  programme code with its fin_source2 label, and the expenditure-type and transfer labels; `seen` lists the years
  whose source carries the code. It is the union of the do-files' `labels_en.do` (2005-2019) and of the LEGEND tabs
  of the five 2020-2024 workbooks (which never conflict except activity 143, named in 2020-2022 and "n/a" in
  2023-2024). Where both name a code the workbooks' text is kept unless it is a placeholder ("n/a", "Activity N (name
  to confirm)"), so three codes read as in the workbooks rather than in the do-files (unit 2234, activity 518,
  sub-paragraph 05.88) and the delivered files' "n/a" for units 2028, 7400, 7500 and 8199, activities 142, 433, 748
  and 802, sub-paragraphs 05.58, 28.10 and 33.07 and programmes 98121-98324 and 99001 is replaced by the name the
  other source has. Hierarchies of codes known only to 2005-2019 follow the do-file rules. The yearly
  `YYYY - legend.json` files in `TXT/` are the individual LEGEND tabs and are no longer read.

## Output columns

`year, admin1, admin2, admin3, func1, func2, func3, econ1, econ2, fin_source1, fin_source2, exp_type, transfer,
adjusted, executed`, in every yearly file. Amounts in BGN, signed. Every classification column carries a label that
opens with its code ("0100 National Assembly", "2.1 Defence", "19.01 Payment of state taxes, penalties and
administrative sanctions"), so the pipeline matches on the code prefix.

## The DLT pipeline (`BGR_transform_load_raw_dlt.py`)

* **Bronze** `bgr_boost_bronze` reads every `YYYY.csv` in `raw_microdata_csv/Bulgaria` (pattern `????.csv`, so a new
  year needs no change) and drops lines without a year.
* **Silver** `bgr_boost_silver` casts the amounts, fills blank labels with "" and builds the BOOST columns. `admin0`
  is Regional for "2 Local" (municipalities) and Central otherwise ("1 Central" and "3 Other", the social security
  funds and, from 2021, the special spending units). `admin1` is "Central Scope" or, for municipal lines, the district
  (oblast) from the unit type, Sofia city apart; `admin2` is the budget unit (ministry, fund, municipality); `geo1`
  follows `admin1`; `is_foreign` is the four external financing sources, matched on the exact label. `func_sub`,
  `func`, `econ_sub` and `econ` are each the workbook's `Executed` sheet SUMIFS criterion (the `EXP_*` code in the
  comment beside every branch) applied to the code prefixes, as inline `when` chains: Public order and safety is the
  union of Judiciary and Public Safety, Social benefits of Social Assistance and Pensions, Interest on debt comes
  first so that an interest line goes nowhere else, and the residuals are General public services and Other
  expenses. The workbook's `road` and `interest` helper flags are the lookups its NOTE sheet defines, activities
  831-834 and 849 and the sub-paragraphs of paragraphs 21-29, applied to the codes.
* **Gold** `bgr_boost_gold` selects the shared columns (`approved` and `revised` are both the adjusted budget; there
  is no separate revised budget) and drops lines with neither amount.

Because the pipeline starts from the rebuilds and not from the workbook's `Expenditure` sheet, its figures differ
from the workbook's `Executed` sheet in known ways: paragraph 19 is signed (the workbook holds its 19.01 executed
amounts of 2014-2019 and its 19.00 adjusted amounts in absolute value, 39 to 809 million BGN a year of executed and
32 to 240 million of adjusted); 2005 is included; the 30 wage, contribution and capital lines of 2023 the workbook
flagged as interest by hand (12.3 million) and the 388 culture lines of 2023 it flagged as road (274.9 million) are
not reproduced; the special units are as the reports print them (2016 +3.9 million, 2019 +145.5 million against the
workbook); the 2020 NHIF row the workbook pasted twice counts once (109.7 million); the budget units' government
level follows the do-files' latest rules (20 million of 2014 and 45 million of 2015 move from Local to Central); and
the two lines the workbook counts in two categories (interest flag, Recreation and Environment) count once.

## BGR_extract_raw_microdata_txt_to_csv_2005_2019.py (2005-2019)

Transcribed step for step from the v1.4 do-file (the code's cell titles keep that order) plus the rules the
v1.5-v1.7 do-files added; the v1.9 do-file only appends 2018 and 2019 to the same steps and reads its special units
from a hand-mapped workbook that is not available here.

1. **Code list**: `labels_en.json`.
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
   multi-code line, a line listing whole paragraphs ("01,02,...") as 11.00; amount = the sum of the special-unit
   columns (budget, National Fund, NAF, other international programmes, other EU funds, third parties), as for
   2020 onward; the 2005-2013 reports print no such columns, only their total "Incl. Special spending units", which
   is used instead (from 2014 that total equals the sum except on 21 lines of 2016); adjusted = executed; lines keyed
   on paragraph and amount because names drift by a row in old reports; from 2019 the whole-function block repeats
   its sub-blocks and is dropped when its total equals theirs; a paragraph total is a subtotal when its sub-lines are
   in the same block. A report missing from `TXT/` is reported and its year gets no special units, so check the log.
5. **Assemble**: rows and special units together; exp_type from the paragraph (1 personnel, 2 recurrent, 3 capital
   incl. 49.02, 4 other); transfer flag for paragraphs 30-32, 60-69, 74-78; unit 7900 is central with its own type 79
   (v1.7); sort; integer codes.
6. **Label and write**: each level's labels are collected from the code list and keyed on the code that opens every
   label; one `YYYY.csv` per year. 7. **Checks**: the yearly executed totals against the "TOTAL EXPENDITURE" of the
   fiscal program printed in the reports (equal to the leva in most years; 2009 -51m and 2014 +24m are in the
   published files too), and the special units against the rows the team mapped by hand for 2005-2017 (totals agree
   to about a million a year; 2016 +4m).

Deliberate departures from the do-files: the special units are parsed from the raw reports with one rule set instead
of taking the team's hand-mapped rows; rows with econ2 4902 get exp_type 3 as the do-files say (the shipped v1.4 file
gave them 2). Apart from the special units the result equals the shipped v1.7 file for 2005-2017 row for row.

## BGR_extract_raw_microdata_txt_to_csv_2020_onward.py (2020 onward)

The years are the extracts found in `TXT/`: every `YYYY_annual_deatiled_data*.txt` of 2020 or later, with the
report `YYYY - special_spending_units*.xls[x]` of the same year ("-final" versions included, earlier versions whose
name carries "v1" ignored; exactly one file of each kind per year, or the notebook stops). A new year is therefore
processed as soon as its two files are in `TXT/`, under the default rules. The default rules are those of the 2023
and 2024 workbooks; 2020, 2021 and 2022 are special cases written as `if YEAR == ...` where they apply, each
reproducing a decision taken for that year's delivered data (`Bulgaria BOOST 2015-2024 expenditure.xlsx`, sheet
2019-24).

1. **Code list**: `labels_en.json`, and the line-name to paragraph table read off the reports that print codes.
2. **Parse the extract**: unit padded to 4 characters; economic code written as "01.01"; the last three digits of
   the activity; ibsf, activity and programme types as integers; rows with an amount flagged.
3. **Clean the expenditure rows**. A paragraph total (XX.00) is the subtotal of the sub-paragraph rows under it and
   must not be counted twice; a paragraph reported without breakdown must be kept. Default: a total is dropped when
   the year's expenditure rows report sub-paragraphs of that paragraph anywhere (equal to "the code list has
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
   sub-blocks'; a line whose code the code list does not know (00-98) left out; a report that prints no paragraph
   codes (2021) takes them from the line-name table of step 1, and 2021 also leaves out two lines of 13 and 37 BGN
   that show as 0.0 thousand.
5. **Label**: units, activities and economic codes from the code list; fin_source1 = the larger of the ibsf and
   activity types (2020: ibsf 3 read as source 10); fin_source2 = the programme code for EU funds, otherwise the
   fin_source2 the code list attaches to the source; a blank expenditure type reads "0"; 2022 labels activity 143
   "n/a". Special units: one unit (9999, "3 Other"; 2020 "1 Central"), state budget, the block's function; 2020
   carries the expenditure type in econ1 and the paragraph label in econ2, 2021 the paragraph label in econ1 and a
   blank econ2, as the delivered data do.
6. **Assemble and write**: expenditure rows then special units (2020 the other way round); one `YYYY.csv` per year.
7. **Check**: each year the delivered workbook covers, against its rows, matched one to one on the codes and amounts
   of the 15 columns (and, for information, on the label text).

Revenue is not produced (the 2023 and 2024 workbooks build a revenue table from the same extracts).

## Why two methods

The old method keeps every paragraph total but blanks its executed amount and keeps adjusted amounts only on
paragraph totals; the new method drops the totals whose sub-paragraphs exist and carries both amounts on every
sub-paragraph row. The old method derives the hierarchy by rules (including admin2 44 "EU funds" for centrally
financed rows with an EU source, about 1,900 rows a year); the new one reads it per code from the code list.
Financing sources, special-unit conventions and year fixes differ too. The two agree on admin1, func1 and func2 for
the 2020-2024 codes except activities 279 and 845 and unit 8199.

## What the paragraph-total rules lose

The workbooks' next-row test compares a total only with the row below it. When the rows without an amount are removed
first, a total is often followed by the same paragraph's total of the next financing source or activity, and is
dropped although nothing under it exists: in 2022, 187 rows of 45.00 and 227 of 51.00, and the eight NHIF 39.00
rows every year. Sorting the rows helps (2021) but does not fix single-code activities such as NHIF. The code-list
rule avoids all that but drops every 40.00 total wherever the list has 40.71 (2021 on): 114-155 million BGN of
scholarships a year. A test that drops a total only when its own unit and activity report a sub-paragraph loses
nothing in any year; it is not applied, so that each year reproduces its delivered data.

## Verification against the delivered files

Row for row, on the codes and the amounts, against `Bulgaria BOOST 2015-2024 expenditure.xlsx` (sheet 2019-24); the
label text differs wherever `labels_en.json` names a code the file leaves as "n/a":

| year | ours | file | identical | difference |
|---|---|---|---|---|
| 2020 | 151,017 | 151,018 | 151,017 | the file's duplicated NHIF row (109.7m) |
| 2021 | 159,509 | 159,509 | 159,509 | none |
| 2022 | 160,459 | 161,059 | 160,459 | the file's 600 zero-amount special-unit lines |
| 2023 | 161,391 | 161,978 | 161,097 | the file's special units are the 2022 block (1,140.9m); 137 keys from the earlier extract version (net 0.99m); 606 zero lines |
| 2024 | 162,626 | 162,626 | 162,618 | programmes 98122 and 98223, "#N/A" in the file, named on our side (8 rows) |

Against `Bulgaria BOOST reduced.xlsx` (Expenditure sheet, aggregated to its columns), executed in million BGN:

| year | ours | file | difference | cause |
|---|---|---|---|---|
| 2006-2013 | | | 0.0 | identical to the leva; in 2006-2011 the file codes part of 10.91 as 10.98, 10.62 as 10.69 and 21.00 as 29.00 (up to 468 million a year, amounts unchanged) and two special-unit lines by the last instead of the first code |
| 2014 | 32,506.3 | 32,545.6 | -39.2 | 19.01 sign flip |
| 2015 | 34,684.6 | 35,493.6 | -809.0 | 19.01 sign flip |
| 2016 | 32,491.5 | 32,634.7 | -143.2 | 19.01 sign flip -147.0; special units +3.8 (police block: the file left out its 8.4 million 19.00 line and booked the printed total on the 21 lines where it differs from the columns) |
| 2017 | 34,471.1 | 34,523.7 | -52.6 | 19.01 sign flip |
| 2018 | 39,515.7 | 39,623.7 | -108.0 | 19.01 sign flip |
| 2019 | 45,201.0 | 45,127.1 | +73.9 | file booked special units from the budget column only (+145.5); 19.01 sign flip -71.6 |
| 2020 | 47,747.7 | 47,857.4 | -109.7 | the file's duplicated NHIF row |
| 2021-2024 | | | 0.0 | identical in amounts (2022, 2023: the file's zero lines and, in 2023, its incremented "N State budget" special-unit labels) |

19.01 "paid taxes, charges and administrative sanctions" carries negative amounts in the extracts; the reduced file
turned them positive (its NOTE sheet: 'some negative values for econ1 "19 paid taxes" were turned positive'), so the
difference is twice the negatives. The reduced file keeps the delivered lines of 2016-2018 and 2021 but aggregates the
other years to its columns; the sign was removed line by line in 2016-2018 and on the aggregated lines in 2014, 2015
and 2019, and every 19.01 amount of 2014-2019 is non-negative there. Its adjusted amounts carry the same flip on the
19.00 paragraph lines (32 to 240 million a year). The delivered files themselves carry the signed amounts in every
year, equal to the extracts (2014 -8.0, 2015 -392.7, 2016 -57.6, 2017 -11.9, 2018 -39.8, 2019 -24.3 million BGN net),
as do the reduced file's 2020-2024 lines.

The reduced file's sheet is our yearly files after these steps: drop 2005, admin2, admin3 and fin_source2; a road
flag on activities 831-834 and 849 and an interest flag on the sub-paragraphs of paragraphs 21-29; one line per
remaining key in every year except 2016-2018 and 2021, which it keeps at line level; func2, econ1 and exp_type cut
to 20 characters in 2006-2008. So built, the two hold the same lines on codes and amounts for 100% of the lines in
2012-2013, 99.7-99.9% in 2006-2011 (the recodes above) and 98.7-99.9% in 2014-2019 (paragraph 19 and the special
units); the label text also differs where the code list names a code the sheet leaves as "n/a" or spells otherwise
("074 Religious activities (unclassified)" in 2007-2008, "062 Environment (unclassified)" in 2015-2016, "42.18 n/a"
in 2016). With the sign flip applied to our lines as well, the two are identical to the leva in 2012-2014 and 2017
(every line), 2018 up to one line (the sheet keeps the agriculture special units' 10.00 total of 93,400 BGN next to
its sub-lines) and 2006-2011 up to the recodes; 2015 differs on 182 lines that net to zero (a few central units
carried under another government level in the sheet); 2016 and 2019 differ by the special-unit choices in the table.

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
* The workbooks' LEGEND tabs file units 7400 (Ministry of Innovation and Growth) and 7500 (Ministry of Electronic
  Governance), created in 2022, as "2 Local" with the districts 74 and 75, and so do the delivered data and the code
  list: 527 lines and 380.7 million BGN of 2022-2024 that the pipeline places under Stara Zagora and Targovishte.

## Open items

* Units 7400 and 7500 above: two entries in `labels_en.json` (admin1 "1 Central", admin2 "11 Ministries and
  agencies") would move them to central government, at the price of those lines no longer matching the delivered
  data on codes.
* Placeholder labels left in `labels_en.json`: "(name to confirm)" on units 2028, 7400, 7500 and 8199 and on
  programme 99001; "n/a" on activities 222, 279 (and its function group 2.7), 625, 845 and 880 (labelled "886 N/A"
  in the workbooks) and on sub-paragraphs 28.20, 28.90 and 40.71; no expenditure type on 28.10, 28.20 and 28.90 and
  "Other" on 98.98, as in the workbooks.
* Bulgaria is not yet in the country list of `cross_country_aggregate_dlt.py`.
