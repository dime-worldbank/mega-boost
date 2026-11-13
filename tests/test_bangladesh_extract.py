"""Integration test for BGD_extract_raw_microdata_excel_to_csv: run the notebook end to end
against fixture iBAS exports and a fixture Keys workbook, and check the per-year CSVs.

All fixture values are dummy/synthetic; labels mirror the real Keys sheets' formats.
"""
import os
import shutil
import time
import unittest

import pandas as pd

from helpers import REPO_ROOT, run_script, write_workbook

NB = "Bangladesh/BGD_extract_raw_microdata_excel_to_csv.py"
TMP = os.path.join(REPO_ROOT, "tests", ".fixtures_tmp", "bgd")
RAW_INPUT_DIR = os.path.join(TMP, "input")
EXPORT_DIR = os.path.join(RAW_INPUT_DIR, "Bangladesh", "other")
KEYS_FILE = os.path.join(TMP, "Bangladesh Keys.xlsx")
OUT_DIR = os.path.join(TMP, "out")

CONTRACT_COLUMNS = [
    "year", "admin1", "admin2", "admin3", "admin4", "geo1", "geo2", "geo3", "budget_type", "fund",
    "econ1", "econ2", "econ3", "econ4", "econ5", "econ6",
    "admin2_standardized", "func1", "func_group", "executed",
]
EXPORT_HEADER = ["ADMIN1", "ADMIN2", "ADMIN3", "ADMIN4", "GEO1", "GEO2", "GEO3", "BUDGETTYPE",
                 "ECON1", "ECON4", "ECON6", "FUND", "EXECUTED"]

WAGES = ("3 - Recurrent Expenditure", "31 - Compensation of Employees", "311 - Wages and Salaries",
         "3111 - Wages and salaries in cash", "31111 - Officers' pay")
ELECTRICITY = ("3 - Recurrent Expenditure", "32 - Purchases of Goods and Services",
               "321 - Administrative Expenses", "3211 - Administrative expenses",
               "32111 - Administrative Expenses")


def keys_sheets(president_func1="Public Services"):
    return {
        # Decoy data sheet, like the real workbook's FY23/FY24 sheets; must be ignored.
        "FY23": [["YEAR", "ADMIN1"], ["2022-23", "CAFO President"]],
        "ECON": [
            ["ECON 6", "ECON 1", "ECON 2", "ECON 3", "ECON 4", "ECON 5", "ECON 6", "Econ 5-Digit"],
            ["3111101-Basic pay (Officer)", *WAGES, "3111101-Basic pay (Officer)", "31111"],
            ["3111101-Basic pay (Officer)", *WAGES, "3111101-Basic pay (Officer)", "31111"],
            ["3211113-Electricity", *ELECTRICITY, "3211113-Electricity", "32111"],
        ],
        "BT-ADM": [
            ["PROPOSED BRIDGE TABLE FOR ADMINISTRATIVE CLASSIFICATION,  AFTER 2018"],
            ["ADMIN2 (as received)", "ADMIN2_STANDARDIZED", "FUNC1 ", "FUNC_GROUP"],
            [101, "101-President's Office", "101-President's Office", president_func1, "Public Sector"],
            [113, "113-Economic Relations Division", "113-Economic Relations Division",
             "Public Services", "Public Sector"],
        ],
    }


def export_row(admin2="101-President's Office\n", econ6="3111101-Basic pay (Officer)", executed=1000):
    return ["CAFO President", admin2, "10101-Personal Division ", "1010101100001-President",
            "30-Dhaka", "3026-Dhaka", "-", "Operating Activity", "3-Recurrent Expenditure",
            "3111-Wages and salaries in cash", econ6, "11001000-Own Source Revenue", executed]


def write_export(filename, title, rows):
    write_workbook(os.path.join(EXPORT_DIR, filename),
                   {"2021-22": [[title], ["(Amount in Taka)"], EXPORT_HEADER, *rows]})


def run():
    return run_script(NB, env={"RAW_INPUT_DIR": RAW_INPUT_DIR, "KEYS_FILE": KEYS_FILE,
                               "OUTPUT_DIR": OUT_DIR})


def read_csv(year):
    return pd.read_csv(os.path.join(OUT_DIR, f"{year}.csv"), dtype={"econ6": str})


def set_mtime(path, offset_seconds):
    t = time.time() + offset_seconds
    os.utime(path, (t, t))


class BangladeshExtractTest(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TMP, ignore_errors=True)
        os.makedirs(EXPORT_DIR)
        write_workbook(KEYS_FILE, keys_sheets())

    def tearDown(self):
        shutil.rmtree(TMP, ignore_errors=True)

    def test_writes_one_csv_per_year_with_contract_columns(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [export_row()])
        write_export("EXP_2022_23.xlsx", "Fiscal Year : 2022-23",
                     [export_row(econ6="3211113-Electricity")])
        run()
        self.assertEqual(sorted(os.listdir(OUT_DIR)), ["2022.csv", "2023.csv"])
        for year in (2022, 2023):
            df = read_csv(year)
            self.assertEqual(list(df.columns), CONTRACT_COLUMNS)
            self.assertEqual(set(df.year), {year})
        self.assertEqual(read_csv(2023).econ2.tolist(), ["32 - Purchases of Goods and Services"])

    def test_admin2_maps_by_three_digit_code(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [
            export_row(admin2="101-President's Office\n"),
            export_row(admin2="11301-Secretariat, Economic Relations Division (ERD)"),
        ])
        run()
        df = read_csv(2022)
        self.assertEqual(df.func1.tolist(), ["Public Services", "Public Services"])
        self.assertEqual(df.admin2_standardized.tolist(),
                         ["101-President's Office", "113-Economic Relations Division"])
        self.assertEqual(df.admin2.tolist(),
                         ["101-President's Office", "11301-Secretariat, Economic Relations Division (ERD)"])

    def test_econ_falls_back_to_five_digit_parent(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22",
                     [export_row(econ6="3111116-Arrear pay")])
        run()
        df = read_csv(2022)
        self.assertEqual(df.econ5.tolist(), ["31111 - Officers' pay"])
        self.assertEqual(df.econ1.tolist(), ["3 - Recurrent Expenditure"])
        self.assertEqual(df.econ6.tolist(), ["3111116-Arrear pay"])

    def test_unmapped_codes_fail_with_list(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [
            export_row(econ6="9999999-Unknown"),
            export_row(admin2="999-Nowhere"),
        ])
        with self.assertRaises(AssertionError) as ctx:
            run()
        self.assertIn("9999999", str(ctx.exception))
        self.assertIn("999-Nowhere", str(ctx.exception))

    def test_blank_and_no_executed_rows_dropped(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [
            export_row(),
            [None] * len(EXPORT_HEADER),
            export_row(executed=None),
        ])
        run()
        self.assertEqual(len(read_csv(2022)), 1)

    def test_duplicate_year_fails(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [export_row()])
        write_export("EXP_2021_22 copy.xlsx", "FISCAL YEAR : 2021-22", [export_row()])
        with self.assertRaises(AssertionError) as ctx:
            run()
        self.assertIn("2022", str(ctx.exception))

    def test_title_without_year_fails(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR :", [export_row()])
        with self.assertRaises(AssertionError) as ctx:
            run()
        self.assertIn("EXP_2021_22.xlsx", str(ctx.exception))

    def test_skips_when_csv_newer_than_inputs(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [export_row(executed=1000)])
        run()
        set_mtime(os.path.join(OUT_DIR, "2022.csv"), 3600)
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [export_row(executed=2000)])
        run()
        self.assertEqual(read_csv(2022).executed.tolist(), [1000])

    def test_rewrites_when_keys_newer(self):
        write_export("EXP_2021_22.xlsx", "FISCAL YEAR : 2021-22", [export_row()])
        run()
        set_mtime(os.path.join(OUT_DIR, "2022.csv"), -3600)
        set_mtime(os.path.join(EXPORT_DIR, "EXP_2021_22.xlsx"), -7200)
        write_workbook(KEYS_FILE, keys_sheets(president_func1="Defence Services"))
        run()
        self.assertEqual(read_csv(2022).func1.tolist(), ["Defence Services"])


if __name__ == "__main__":
    unittest.main()
