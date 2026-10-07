"""TGO_ETL and TGO_aggregate with DB_BACKEND=postgres give the same tables as the CSV path.

Both scripts run twice on the same synthetic inputs, once writing CSVs and once
writing to PostgreSQL, and every table is compared. Needs a PostgreSQL database named
prd_mega and psycopg; the tests are skipped unless TEST_POSTGRES_DSN points at one, e.g.

    docker run -d -e POSTGRES_PASSWORD=test -e POSTGRES_DB=prd_mega -p 5432:5432 postgres:16
    pip install "psycopg[binary]"
    cd tests && TEST_POSTGRES_DSN=postgresql://postgres:test@localhost:5432/prd_mega python -m unittest test_togo_postgres

They drop and refill the schemas boost_intermediate, indicator and boost of that database.
"""
import os
import shutil
import sys
import unittest

import pandas as pd

from helpers import REPO_ROOT, run_script, write_workbook
from test_togo_etl import COLS, ROWS_2021, ROWS_2022, SHEET_NAME

DSN = os.environ.get("TEST_POSTGRES_DSN")
TMP = os.path.join(REPO_ROOT, "tests", ".fixtures_tmp", "togo_postgres")
REGIONS = ["Centrale", "Kara", "Maritime", "Plateaux", "Savanes"]
YEARS = [2021, 2022]

# One small table per indicator TGO_aggregate joins, for Togo plus a row of another
# country that the script must ignore. Values are synthetic.
INDICATORS = {
    "consumer_price_index": pd.DataFrame({
        "country_name": ["Togo", "Togo", "Ghana"], "country_code": ["TGO", "TGO", "GHA"],
        "year": [2021, 2022, 2021], "cpi": [100.0, 108.0, 90.0]}),
    "population": pd.DataFrame({
        "country_name": ["Togo", "Togo", "Ghana"], "year": [2021, 2022, 2021],
        "population": [8.5e6, 8.7e6, 3e7]}),
    "subnational_population": pd.DataFrame({
        "country_name": "Togo", "adm1_name": REGIONS * 2, "year": [2021] * 5 + [2022] * 5,
        "population": [1_000_000 + 10_000 * i for i in range(10)], "data_source": "synthetic"}),
    "poverty_rate": pd.DataFrame({
        "country_name": ["Togo", "Togo"], "country_code": ["TGO", "TGO"], "region": ["SSF", "SSF"],
        "income_level": ["LMC", "LMC"], "year": YEARS, "poor300": [25.0, 24.0],
        "poverty_rate": [40.0, None], "data_source": ["synthetic", "synthetic"]}),
    "global_data_lab_hd_index": pd.DataFrame({
        "country_name": "Togo", "adm1_name": REGIONS * 2, "year": [2021] * 5 + [2022] * 5,
        "health_index": [0.5 + 0.01 * i for i in range(10)],
        "attendance_6to17yo": [0.7 - 0.01 * i for i in range(10)]}),
    "edu_spending": pd.DataFrame({
        "country_name": ["Togo", "Togo"], "year": YEARS, "edu_spending_current_lcu_icp": [1e6, 1.1e6]}),
    "health_expenditure": pd.DataFrame({
        "country_name": ["Togo", "Togo"], "year": YEARS, "che": [5000.0, 6000.0],
        "oop_percent_che": [40.0, 38.5]}),
}

OUTPUTS = [
    "pov_expenditure_by_country_year",
    "expenditure_by_country_func_econ_year",
    "expenditure_by_country_geo0_func_sub_year",
    "expenditure_by_country_geo1_year",
    "expenditure_and_outcome_by_country_geo1_func_year",
    "edu_private_expenditure_by_country_year",
    "health_private_expenditure_by_country_year",
    "data_availability",
]


def sorted_frame(df):
    df = df.reset_index(drop=True)
    return df.sort_values(list(df.columns), na_position="first").reset_index(drop=True)


@unittest.skipUnless(DSN, "TEST_POSTGRES_DSN is not set")
class TogoPostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, os.path.join(REPO_ROOT, "Togo"))
        import postgres_tables
        import psycopg
        cls.pg = postgres_tables
        os.environ["POSTGRES_DSN"] = DSN
        with psycopg.connect(DSN, autocommit=True) as conn:
            for schema in ("boost_intermediate", "indicator", "boost"):
                conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

        shutil.rmtree(TMP, ignore_errors=True)
        input_dir = os.path.join(TMP, "input")
        cls.csv_out = os.path.join(TMP, "csv")
        cls.indicator_dir = os.path.join(TMP, "indicator")
        cls.aggregate_out = os.path.join(TMP, "aggregate")
        for d in (input_dir, cls.csv_out, cls.indicator_dir):
            os.makedirs(d)
        write_workbook(os.path.join(input_dir, "BUDGET.xlsx"), {SHEET_NAME: [COLS] + ROWS_2021 + ROWS_2022})
        for name, df in INDICATORS.items():
            df.to_csv(os.path.join(cls.indicator_dir, f"{name}.csv"), index=False)
            postgres_tables.replace_table(df, "prd_mega", "indicator", name)

        etl_env = {"INPUT_DIR": input_dir, "OUTPUT_DIR": cls.csv_out}
        run_script("Togo/TGO_ETL.py", env=etl_env)
        run_script("Togo/TGO_ETL.py", env={**etl_env, "OUTPUT_DIR": os.path.join(TMP, "bronze_pg"),
                                            "DB_BACKEND": "postgres", "POSTGRES_DSN": DSN})
        run_script("Togo/TGO_aggregate.py", env={
            "GOLD_CSV": os.path.join(cls.csv_out, "tgo_boost_gold.csv"),
            "INDICATOR_DIR": cls.indicator_dir, "OUTPUT_DIR": cls.aggregate_out})
        run_script("Togo/TGO_aggregate.py", env={"DB_BACKEND": "postgres", "POSTGRES_DSN": DSN})

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(TMP, ignore_errors=True)

    def test_gold_matches_csv(self):
        csv = pd.read_csv(os.path.join(self.csv_out, "tgo_boost_gold.csv"))
        pg = self.pg.read_table("prd_mega", "boost_intermediate", "tgo_boost_gold")
        pd.testing.assert_frame_equal(sorted_frame(pg), sorted_frame(csv), check_dtype=False)
        import psycopg
        with psycopg.connect(DSN) as conn:
            types = dict(conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'boost_intermediate' AND table_name = 'tgo_boost_gold'").fetchall())
        self.assertEqual(
            {c: types[c] for c in ("year", "admin2", "is_foreign", "executed")},
            {"year": "bigint", "admin2": "text", "is_foreign": "boolean", "executed": "double precision"})

    def test_silver_keeps_columns_and_code_text(self):
        csv = pd.read_csv(os.path.join(self.csv_out, "tgo_2021_onward_boost_silver.csv"))
        pg = self.pg.read_table("prd_mega", "boost_intermediate", "tgo_2021_onward_boost_silver")
        self.assertEqual(list(pg.columns), list(csv.columns))
        self.assertEqual(len(pg), len(csv))
        self.assertEqual(sorted(pg["CODE_FUNC1"]), [f"{i:02d}" for i in range(1, 11)])

    def test_dashboard_tables_match_csv(self):
        for name in OUTPUTS:
            with self.subTest(table=name):
                csv = pd.read_csv(os.path.join(self.aggregate_out, f"{name}.csv"))
                pg = self.pg.read_table("prd_mega", "boost", name)
                self.assertGreater(len(pg), 0)
                pd.testing.assert_frame_equal(sorted_frame(pg), sorted_frame(csv), check_dtype=False)


if __name__ == "__main__":
    unittest.main()
