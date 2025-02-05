# mega-boost

ETL pipelines that take BOOST-coded/raw microdata for each country, clean, harmonize, and store them in a single Delta table. Aggregated budget and expenditure data at various levels are also created for query, visualization, and reporting purposes.

## Cross-Country Harmonization

BOOST data is country-specific — for instance, some ministries of finance have IT systems that record detailed expenditure data for every provider of public services, while others may only have data aggregated at the district levels. The harmonization work in this repo standardizes the labeling of common BOOST features on the fiscal data at the most disaggregated level available to maximize the flexibility for analysis. The resulting data is stored in a table named `boost_gold`. The column definitions are detailed at the bottom of this file.

## Code Organization

- Each country's ETL pipeline code resides in its own folder.
- [cross_country_aggregate_dlt.py](./cross_country_aggregate_dlt.py) vertically stacks all countries' micro-level data into the `boost_gold` table and creates aggregates at various levels.
- [/quality](./quality/) contains the ETL code for processing BOOST Cross Country Interface (CCI) Excel files' "Approved" and "Executed" sheets for all available countries. The resulting tables are used for both:
  - Programmatic quality checks in [cross_country_aggregate_dlt.py](./cross_country_aggregate_dlt.py), and
  - Manual review and resolution of discrepancies along critical dimensions (total, functional, economic, etc.) in the [Quality Dashboard](https://adb-6102124407836814.14.azuredatabricks.net/dashboardsv3/01ef07bf07d615bb98b5ff5b37e6fa69/published)

## How to Add a New Country

1. Create a new folder named using the country name.
2. Write and test the country-specific ETL code. You may organize the code as follows:
  - A notebook for extracting the BOOST/raw data from the Excel sheet(s) (e.g. [ALB_extract_microdata_excel_to_csv](./Albania/ALB_extract_microdata_excel_to_csv.py)). The resulting CSV files can then be loaded directly by a subsequent Delta Live Table (DLT) script (described in the next point). If the raw data is already in a [format supported by DLT load](https://docs.databricks.com/en/delta-live-tables/load.html), such as CSV, you may skip this step/notebook altogether. To test the code, run the notebook in Databricks with the assigned project compute cluster (e.g. DAP-PROD-CLUSTER).
  - A DLT notebook for cleaning and transforming the BOOST/raw data. The resulting data should have each line item tagged with a predefined set of tags in a consistent manner (e.g. [ALB_transform_load_dlt](./Albania/ALB_transform_load_dlt.py)). To validate the DLT code and verify the resulting table schema, run the notebook with the default cluster as you would a normal notebook. To populate the tables, create a DLT pipeline in the `unity catalog`, `prg_mega` as the default catalog, and `boost_intermediate` as the target schema name. Reference an existing country's DLT pipeline settings for other configurations.
  - Be sure to follow the naming convention (code folder & file names, table names, pipeline names, etc.) referencing existing countries. When a country code is needed, use the [3-letter ISO 3166 country codes](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3).
3. Add the new country's ETL pipeline steps to the "BOOST Harmonize" job: Workflows > Jobs > BOOST Harmonize > Tasks > + Add task
  - Add the extraction step using Type: Notebook, and Source: git. Use the default cluster as the compute.
  - Add the DLT step using Type: Delta Live Table pipeline, and select your DLT pipeline.
  - Check that the step dependencies are configured correctly.
4. Source the official data, code, and add the ETL pipeline for the country's admin1 level subnational population. Skip this step if the BOOST country data does not have subnational tagging.
  - Find official statistics and projections from the country's national statistics office if available. Otherwise, check if [census.gov](https://www.census.gov/geographies/mapping-files/time-series/demo/international-programs/subnationalpopulation.html) might provide subnational population statistics for the given country. Nigeria's subnational population estimates come from census.gov, for [example](https://github.com/dime-worldbank/mega-indicators/blob/main/population/NGA/nga_subnational_population.py).
  - Add the ETL code to the [mega-indicator repo](https://github.com/weilu/mega-indicators/tree/main/population) because it's a public good by itself. Make sure the region names in the subnational population dataset align with the BOOST subnational names for the given country, as the two datasets will be joined on the subnational region names.
  - Add the country's ETL script to the subnational population pipeline "Indicators on Demand": Workflow > Jobs > Indicators on Demand > Tasks > + Add task.
  - Global Data Lab's [subnational human development datasets](https://globaldatalab.org/shdi/table/) are used as some outcome indicators. Align subnational region names with those of population and BOOST if necessary.
5. Add the country to the cross-country aggregation DLT pipeline
  - Update [cross_country_aggregate_dlt.py](./cross_country_aggregate_dlt.py) to add the intermediate table (containing only the new country's cleaned microdata) as a source for stacking into `boost_gold`.
  - Execute the "BOOST Agg" DLT workflow to perform the stacking and aggregation: Workflows > Delta Live Tables > BOOST Agg > Start.
  - The BOOST Agg pipeline has built-in quality checks. If the job fails due to failed quality checks, investigate and fix them.
6. Check, investigate, and resolve discrepancies with the pre-existing BOOST data workflow
  - Visit the [PFM BOOST Data Quality Dashboard](https://adb-6102124407836814.14.azuredatabricks.net/dashboardsv3/01ef07bf07d615bb98b5ff5b37e6fa69/published).
  - Filter Countries to the newly added country. Check that the year coverage range and number of functional & economic category coverage are as expected.
  - Further filter by Discrepancy %, set min as 1% and 5%, and investigate discrepancies at various dimensions (e.g., func/econ/total). Resolution may involve an iterative process of updating the pre-existing BOOST Excel workflows and/or updating the ETL pipeline code.
  - Our current acceptable threshold of discrepancy is 5%. If there needs to be a fix in the BOOST CCI EXCEL sheets, docuemnt the content of the changes and leave the logs in the following document: [BOOST CCI changes reconciliations](https://worldbankgroup-my.sharepoint.com/:x:/r/personal/wlu4_worldbank_org/_layouts/15/Doc.aspx?sourcedoc=%7B19BCE6C7-E183-4F11-A41F-E4BBB792ABFC%7D&file=BOOST%20CCI%20changes%20reconciled.xlsx&action=default&mobileredirect=true).
  - Make sure that the EXCEL is updated at both [BOOST's shared project folder](https://worldbankgroup-my.sharepoint.com/:f:/r/personal/mmastruzzi_worldbank_org/Documents/BOOST/Cross-Country%20Interface/Countries?csf=1&web=1&e=eUcv12) and [MEGA project folder for databricks processing](https://worldbankgroup.sharepoint.com/:f:/r/sites/dap/BOOSTProcessed/Shared%20Documents/input/Countries?csf=1&web=1&e=ArX7md)
7. Check the [PowerBI report](https://app.powerbi.com/groups/75fff923-5acd-443e-877b-d2c6e88cdb31/reports/a28af24a-6a8a-4241-bd42-40a4c4af5716/) to ensure the new country is reflected and its narratives are correctly presented.

## boost_gold

| Column Name | Data Type | Description                                                                 | Possible Values                       | Possible Value Catalog Query           |
|-------------|-----------|-----------------------------------------------------------------------------|---------------------------------------|----------------------------------------|
| country_name| string    | The name of the country for which the budget data is recorded               | e.g., "Kenya", "Brazil". See [wbgapi](https://pypi.org/project/wbgapi/) or [WB website](https://data.worldbank.org/country) for a full list | `SELECT DISTINCT country_name FROM indicator.country` |
| year        | int       | The fiscal year for the budget data                                         | e.g., 2023, 2024                      | |
| admin0      | string    | Who spent the money at the highest administrative level                     | Either "Central" or "Regional"        | |
| admin1      | string    | Who spent the money at the first sub-national administrative level          | If admin0 is "Regional," this is the state or province name; if admin0 is "Central," this is always "Central Scope." "Scope" is postfixed to avoid conflicting with a subnational region named "Central" in some countries | `SELECT DISTINCT admin1_region FROM indicator.admin1_boundaries_gold` |
| admin2      | string    | Who spent the money at the second sub-national administrative level         | For "Central" spending, this is the government agency/ministry name; for "Regional" spending, this can be the district or municipality name or subnational government agency/department name | |
| geo1        | string    | Geographically, at which first sub-national administrative level the money was spent | Same as admin1 possible values | `SELECT DISTINCT admin1_region FROM indicator.admin1_boundaries_gold` |
| func        | string    | Functional classification of the budget                                     | e.g., Health, Education. See [Classification of the Functions of Government (COFOG)](https://en.wikipedia.org/wiki/Classification_of_the_Functions_of_Government) for a full list | `SELECT DISTINCT func FROM boost_intermediate.quality_functional_gold` |
| func_sub    | string    | Sub-functional classification under the main COFOG function                 | e.g., "primary education", "secondary education" | |
| econ        | string    | Economic classification of the budget                                       | e.g., "Wage bill", "Goods and services" | `SELECT DISTINCT econ FROM boost_intermediate.quality_economic_gold` |
| econ_sub    | string    | Sub-economic classification under the main economic category                | e.g., "basic wages", "allowances" | |
| approved    | double    | Amount of budget in current local currency approved by the relevant authority | Numeric value representing local currency | |
| revised     | double    | Revised budget in current local currency amount during the fiscal year      | Numeric value representing local currency | |
| executed    | double    | Actual amount spent in current local currency by the end of the fiscal year | Numeric value representing local currency | |

