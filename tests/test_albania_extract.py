"""Integration test for ALB_extract_raw_microdata_excel_to_csv: run the notebook end to
end against a fixture input tree (a multi-file 2024 year and a single-workbook 2025 year)
and compare each produced CSV to a committed golden file.

All fixture values are dummy/synthetic, chosen only to satisfy the column-format validators.

Set WRITE_GOLDEN=1 to (re)generate the golden CSVs after an intentional change.
"""
import os
import shutil
import unittest

from helpers import REPO_ROOT, run_script, write_workbook

NB = "Albania/ALB_extract_raw_microdata_excel_to_csv.py"
FIXTURES = os.path.join(REPO_ROOT, "tests", "fixtures", "albania")
TMP = os.path.join(REPO_ROOT, "tests", ".fixtures_tmp", "alb")
INPUT_ROOT = os.path.join(TMP, "input")
OUT_DIR = os.path.join(TMP, "out")

# 2025: single consolidated workbook, one sheet per category (dummy data).
SINGLE_SHEETS = {
    "Te ardhura 7shif": [  # revenue: 6 real cols + trailing empties; non-46655 accounts
        [None, None, None, None, None, None, "title", None],
        ["Gov", "Line Ministry", "Institucion", "Account", "TDO", "FYTD Actual", None, None],
        ["100", "20", "3000001", "5000001", "6000", "111111", None, None],
        ["100", "20", "3000002", "5000002", "6000", "222222", None, None],
    ],
    "Shpenz 3shifror": [  # 3-digit expense: 11 cols
        [None] * 11,
        ["Gov", "Line Ministry", "Institucion", "Chapter", "Progr", "Account", "TDO", "Project", "Fakti", "Buxheti Operativ", "Buxheti Fillestar"],
        ["100", "20", "3000001", "40", "F0001", "500", "6000", "P000001", "0", "111", "111"],
        ["100", "20", "3000001", "40", "F0001", "5000", "6000", "P000002", "0", "222", "222"],  # econ3 len 4 -> dropped
        [None, None, None, None, None, None, None, "Total", "x", "y", "z"],  # footer -> dropped
    ],
    "46655 niv 7shifror": [  # 46655: 6 real cols; accounts are 46655-prefixed
        [None, None, None, None, None, None, "title"],
        ["Gov", "Line Ministry", "Institucion", "Account", "TDO", "FYTD Actual", None],
        ["100", "20", "3000001", "4665501", "6000", "333333", None],
        ["100", "20", "3000001", "4665502", "6000", "444444", None],
    ],
    "Shpenz 7 shifror": [  # 7-digit expense: 11 cols with 2 description columns (one-word
                           # values on purpose, to guard the positional column drop)
        [None] * 11,
        ["Gov", "Line Ministry", "Institucion", "Desc1", "Chapter", "Progr", "Account", None, "TDO", "Project", "FYTD Actual"],
        ["100", "20", "3000001", "InstDesc", "40", "F0001", "5000001", "AcctDesc", "6000", "P00001", "111111"],
        ["100", "20", "3000001", "InstDesc", "40", "F0001", "5000002", "AcctDesc", "6000", "P00002", "222222"],
    ],
}

# 2024: one workbook per category (filenames carry the substrings the dispatch matches).
MULTI_FILES = {
    "BOOST Expenditure 7 digit level.xlsx": {"Sheet1": [
        [None] * 11,
        ["Gov Entity", "Ministry", "Inst.", "Institution Description", "Chapter", "Progr", "Account", "Account Description", "TDO", "Project", "FYTD Actual"],
        ["100", "20", "3000001", "InstDesc", "40", "F0001", "5000001", "AcctDesc", "6000", "P00001", 111111],
        ["100", "20", "3000001", "InstDesc", "40", "F0001", "5000002", "AcctDesc", "6000", "P00002", 222222],
    ]},
    "BOOST Expenditure 3 digit level.xlsx": {"Sheet1": [
        ["Gov", "Line Ministri", "Inst.", "Chapter", "Progr", "Account", "TDO", "Project", "Actual", "Operational Budget", "Initial Budget"],
        ["100", "20", "3000001", "40", "F0001", "500", "6000", "P000001", 0, 111, 111],
        ["100", "20", "3000001", "40", "F0001", "5000", "6000", "P000002", 0, 222, 222],  # econ3 len 4 -> dropped
    ]},
    "BOOST Revenue 7 digit level.xlsx": {"Sheet1": [
        ["Gov", "LM", "Inst.", "Account", "TDO", "FYTD Actual"],
        ["100", "20", "3000001", "5000001", "6000", 111111],
    ]},
    "BOOST 46655 Account 7 digit level.xlsx": {"Sheet1": [
        ["Gov", "LM", "Inst.", "Account", "TDO", "FYTD Actual"],
        ["100", "20", "3000001", "4665501", "6000", 333333],
    ]},
}


class AlbaniaExtractTest(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TMP, ignore_errors=True)
        y2024 = os.path.join(INPUT_ROOT, "Albania", "2024")
        y2025 = os.path.join(INPUT_ROOT, "Albania", "2025")
        os.makedirs(y2024)
        os.makedirs(y2025)
        for name, sheets in MULTI_FILES.items():
            write_workbook(os.path.join(y2024, name), sheets)
        write_workbook(os.path.join(y2025, "BOOST 12-2025.xlsx"), SINGLE_SHEETS)

    def tearDown(self):
        shutil.rmtree(TMP, ignore_errors=True)

    def test_extraction_matches_golden(self):
        run_script(NB, env={"RAW_INPUT_DIR": INPUT_ROOT, "OUTPUT_DIR": OUT_DIR})

        for name in ["2024.csv", "2024_rev.csv", "2025.csv", "2025_rev.csv"]:
            with open(os.path.join(OUT_DIR, name)) as fh:
                actual = fh.read()
            golden = os.path.join(FIXTURES, name)
            if os.environ.get("WRITE_GOLDEN"):
                os.makedirs(FIXTURES, exist_ok=True)
                with open(golden, "w") as fh:
                    fh.write(actual)
            with open(golden) as fh:
                self.assertEqual(actual, fh.read(), f"{name} differs; set WRITE_GOLDEN=1 to refresh if intended")


if __name__ == "__main__":
    unittest.main()
