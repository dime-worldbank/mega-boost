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
# INPUT_DIR holds the raw .xlsx workbook(s); each file's sheet with the most data is used
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