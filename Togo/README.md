## ETL Script for Togo BOOST

[TGO_ETL.py](Togo/TGO_ETL.py) is designed to be executable both on databricks and in a regular python environment. See the main [README](README.md) for databricks instructions. Below are instructions for development and execution in a regular python environment without databricks.

### Requirements (without databricks)
- python3
- pip
- optional: [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/) or [virtualenv](https://virtualenv.pypa.io/en/latest/)

### Setup (without databricks)

```
# optional: if using virtualenvwrapper:
# mkvirtualenv togo_boost

# install depedencies
pip install numpy pandas openpyxl

# export input & output directory information based on local setup
# without the env var exports the script will prompt for user input on every script run
# INPUT_DIR holds the raw .xlsx workbook(s); each file's sheet with the most data is used.
# If a year appears in several workbooks, only the rows of the file with the most executed spending (ORDONNANCER) for that year are used.
export INPUT_DIR='/path/to/raw/data/dir'
export OUTPUT_DIR='/path/to/output/dir/'
```

### Run (without databricks)

```
python TGO_ETL.py
```

Alternatively, the script can also be imported into Jupyter and executed as a notebook.

### Running tests (without databricks)

Integration tests run `TGO_ETL.py` end to end against a small fixture workbook via its non-databricks path. From the repo-level [`tests/`](../tests/) folder, run only the Togo tests:

```
cd tests && python -m unittest test_togo_etl
```

## Aggregation Script for the Dashboard

[TGO_aggregate.py](TGO_aggregate.py) builds the tables the RPF country dashboard reads, from the gold table written by `TGO_ETL.py` and CSV extracts of the indicator tables it joins. It runs in a regular python environment without databricks or spark; no quality checks are run.

### Inputs
- `tgo_boost_gold.csv` from the `OUTPUT_DIR` of `TGO_ETL.py`
- a folder with one CSV per `prd_mega.indicator` table, named after the table: `consumer_price_index`, `population`, `subnational_population`, `poverty_rate`, `global_data_lab_hd_index`, `edu_spending`, `health_expenditure`. Export them from databricks (rows of other countries are ignored). The CPI file must include the earliest year of the gold table, which is the base year of the deflator.

### Run (without databricks)

```
# without the env var exports the script will prompt for the three paths
export GOLD_CSV='/path/to/output/dir/tgo_boost_gold.csv'
export INDICATOR_DIR='/path/to/indicator/csvs/'
export OUTPUT_DIR='/path/to/aggregate/output/'

python TGO_aggregate.py
```

### Outputs
One CSV per table the dashboard reads, in `OUTPUT_DIR`: `pov_expenditure_by_country_year`, `expenditure_by_country_func_econ_year`, `expenditure_by_country_geo0_func_sub_year`, `expenditure_by_country_geo1_year`, `expenditure_and_outcome_by_country_geo1_func_year`, `edu_private_expenditure_by_country_year`, `health_private_expenditure_by_country_year` and `data_availability`.

