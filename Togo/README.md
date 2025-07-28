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

# export input file & output directory information based on local setup
# without the env var exports the script will prompt for user input on every script run
export INPUT_FILE_NAME='/path/to/raw/data.xlsx'
export INPUT_SHEET_NAME='Sheet1'
export OUTPUT_DIR='/path/to/output/dir/'
```

### Run (without databricks)

```
python TGO_ETL.py
```

Alternativley, the script can also be imported into Jupyter and executed as a notebook.