"""Integration test for TGO_ETL: run the notebook end to end via its non-Databricks
(pure pandas, no Spark) path against a fixture *directory*, and compare the produced CSVs
to committed golden files.

The fixture dir holds two workbooks (one with a decoy sheet) spanning two years, so this
exercises the shared discovery (glob + largest sheet + concat), the per-year bronze split,
and the silver/gold build. All values are dummy/synthetic.

Set WRITE_GOLDEN=1 to (re)generate the golden CSVs after an intentional change.
"""
import os
import shutil
import unittest

from helpers import REPO_ROOT, run_script, write_workbook

NB = "Togo/TGO_ETL.py"
FIXTURES = os.path.join(REPO_ROOT, "tests", "fixtures", "togo")
TMP = os.path.join(REPO_ROOT, "tests", ".fixtures_tmp", "togo")

COLS = ["YEAR", "ADMIN2", "ADMIN4", "ADMIN5", "REGION", "PREFECTURE",
        "CODE_FUNC1", "CODE_FUNC2", "CODE_FUNC3",
        "CODE_ECON1", "CODE_ECON2", "CODE_ECON3", "CODE_ECON4", "CODE_ADMIN4",
        "ORDONNANCER", "DOTATION_INITIALE", "DOTATION_FINALE"]

# Dummy rows. CODE_FUNC1 01-10 and the six REGION values are required by the silver asserts;
# the ADMIN5 financing label drives is_foreign; CODE_ECON1 1-5 varies the economic class.
# Everything else is synthetic. Years 2021/2022 so the per-year bronze split is exercised.
ROWS_2021 = [
    [2021, "A00", "Unit 0", "FINANCEMENTS INTERNES", "REGION CENTRALE",     "Pref 0", "01", "X00", "Y000", "1", "E0", "E000", "E00000", "D000000000000", 100, 110, 105],
    [2021, "A01", "Unit 1", "FINANCEMENTS INTERNES", "REGION DE LA KARA",   "Pref 1", "02", "X00", "Y000", "2", "E0", "E000", "E00000", "D000000000001", 200, 210, 205],
    [2021, "A02", "Unit 2", "FINANCEMENTS INTERNES", "REGION MARITIME",     "Pref 2", "03", "X00", "Y000", "3", "E0", "E000", "E00000", "D000000000002", 300, 310, 305],
    [2021, "A03", "Unit 3", "FINANCEMENTS INTERNES", "REGION DES PLATEAUX", "Pref 3", "04", "X00", "Y000", "4", "E0", "E000", "E00000", "D000000000003", 400, 410, 405],
    [2021, "A04", "REHABILITATION unit", "FINANCEMENTS INTERNES", "REGION DES SAVANES", "Pref 4", "05", "X00", "Y000", "5", "E0", "E000", "E00000", "D000000000004", 500, 510, 505],
]
ROWS_2022 = [
    [2022, "A05", "Unit 5", "FINANCEMENTS INTERNES", "AUTRES REGIONS", "Pref 5", "06", "X00", "Y000", "1", "E0", "E000", "E00000", "D000000000005", 600, 610, 605],
    [2022, "A06", "Unit 6", "FINANCEMENTS INTERNES", "AUTRES REGIONS", "Pref 6", "07", "X00", "Y000", "2", "E0", "E000", "E00000", "D000000000006", 700, 710, 705],
    [2022, "A07", "Unit 7", "FINANCEMENTS INTERNES", "AUTRES REGIONS", "Pref 7", "08", "X00", "Y000", "3", "E0", "E000", "E00000", "D000000000007", 800, 810, 805],
    [2022, "A08", "Unit 8", "FINANCEMENTS INTERNES", "AUTRES REGIONS", "Pref 8", "09", "X00", "Y000", "4", "E0", "E000", "E00000", "D000000000008", 900, 910, 905],
    [2022, "A09", "Unit 9", "FINANCEMENTS EXTERNES", "AUTRES REGIONS", "Pref 9", "10", "X00", "Y000", "5", "E0", "E000", "E00000", "D000000000009", 1000, 1010, 1005],
]
SHEET_NAME = "2021_A_2025"


class TogoEtlTest(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TMP, ignore_errors=True)
        self.input_dir = os.path.join(TMP, "input")
        self.out = os.path.join(TMP, "out")
        os.makedirs(self.input_dir)
        os.makedirs(self.out)
        # Two workbooks read in sorted order; part1 also has a smaller decoy sheet so
        # largest_sheet must pick the data sheet over it. Split by year across the two files.
        write_workbook(os.path.join(self.input_dir, "BUDGET part1.xlsx"), {
            "decoy": [["ignore", "me"], ["1", "2"]],
            SHEET_NAME: [COLS] + ROWS_2021,
        })
        write_workbook(os.path.join(self.input_dir, "BUDGET part2.xlsx"), {
            SHEET_NAME: [COLS] + ROWS_2022,
        })

    def tearDown(self):
        shutil.rmtree(TMP, ignore_errors=True)

    def _assert_golden(self, filename):
        with open(os.path.join(self.out, filename)) as fh:
            actual = fh.read()
        golden = os.path.join(FIXTURES, filename)
        if os.environ.get("WRITE_GOLDEN"):
            os.makedirs(FIXTURES, exist_ok=True)
            with open(golden, "w") as fh:
                fh.write(actual)
        with open(golden) as fh:
            self.assertEqual(actual, fh.read(), f"{filename} differs; set WRITE_GOLDEN=1 to refresh if intended")

    def test_outputs_match_golden(self):
        run_script(NB, env={"INPUT_DIR": self.input_dir, "OUTPUT_DIR": self.out})
        # per-year bronze split (filenames + content) and the final gold table
        self._assert_golden("2021.csv")
        self._assert_golden("2022.csv")
        self._assert_golden("tgo_boost_gold.csv")


if __name__ == "__main__":
    unittest.main()
