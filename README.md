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
##### 0. Clone the repository (if not already done)
In your Workspace in the top right click `Create`->`Git Folder`. The path to the git repository is `https://github.com/dime-worldbank/mega-boost.git`
![image](https://github.com/user-attachments/assets/bc28adea-89e7-4bc6-b172-aed3708ba46a)
##### 1. Create a new branch for your country
- Navigate into the directory 
- Click on the tag for the Main branch
- Click `create branch`
##### 2. Create a new folder named using the country name (see image below).
![image](https://github.com/user-attachments/assets/d5118b17-519d-4094-a3dc-9cc3d41a92ce)
##### 3. Locate the source data in the volume
![image](https://github.com/user-attachments/assets/70619e3c-c867-4356-b967-5e6c256a71b3)
This is the path you will use in your ETL. The root for the volume with country data can be found [here](https://adb-6102124407836814.14.azuredatabricks.net/explore/data/volumes/prd_mega/sboost4/vboost4?o=6102124407836814&volumePath=%2FVolumes%2Fprd_mega%2Fsboost4%2Fvboost4%2FDocuments%2Finput%2FCountries%2F).
##### 4. Write a notebook to extract raw data
  -  If the raw data is already in a [format supported by DLT load](https://docs.databricks.com/en/delta-live-tables/load.html), such as CSV, you may skip this step/notebook altogether. This is required for .xlsx files
  - Name the file 
    - Prefix the file name with the three letter country code of your country (so the boost pipelines can pick it up later). [3-letter ISO 3166 country codes](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3).
    - Use the following suffix: `_extract_microdata_excel_to_csv`. see below example.
    - <img width="267" alt="image" src="https://github.com/user-attachments/assets/b8c89132-6935-48fd-a9c8-dd5388117d3b" />
  - Extract the BOOST/raw data from the Excel sheet(s) (e.g. [ALB_extract_microdata_excel_to_csv](./Albania/ALB_extract_microdata_excel_to_csv.py)). The resulting CSV files can then be loaded directly by a subsequent Delta Live Table (DLT) script (described later).
##### 5. Ingest subnational population data. See [mega-boost-indicators](https://github.com/dime-worldbank/mega-indicators) for more documentation.
  -  Find official statistics and projections from the country's national statistics office if available. Otherwise, check if [census.gov](https://www.census.gov/geographies/mapping-files/time-series/demo/international-programs/subnationalpopulation.html) might provide subnational population statistics for the given country. Nigeria's subnational population estimates come from census.gov, for [example](https://github.com/dime-worldbank/mega-indicators/blob/main/population/NGA/nga_subnational_population.py).
  -  Add the ETL code to the [mega-indicator repo](https://github.com/weilu/mega-indicators/tree/main/population) because it's a public good by itself. Make sure the region names in the subnational population dataset align with the BOOST subnational names for the given country, as the two datasets will be joined on the subnational region names.
  -  Add the country's ETL script to the subnational population pipeline "Indicators on Demand": Workflow > Jobs > Indicators on Demand > Tasks > + Add task.
  -  Global Data Lab's [subnational human development datasets](https://globaldatalab.org/shdi/table/) are used as some outcome indicators. Align subnational region names with those of population and BOOST if necessary.
##### 6. Write a notebook to transform the data (i.e. emulate the calculations in the Excel/CSV document)
  - A DLT notebook for cleaning and transforming the BOOST/raw data. The resulting data should have each line item tagged with a predefined set of tags in a consistent manner (e.g. [ALB_transform_load_dlt](./Albania/ALB_transform_load_dlt.py)). To validate the DLT code and verify the resulting table schema, run the notebook with the default cluster as you would a normal notebook. To populate the tables, create a DLT pipeline in the `unity catalog`, `prg_mega` as the default catalog, and `boost_intermediate` as the target schema name. Reference an existing country's DLT pipeline settings for other configurations.
  - Be sure to follow the naming convention (code folder & file names, table names, pipeline names, etc.) referencing existing countries. When a country code is needed, use the [3-letter ISO 3166 country codes](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3).
  - Test the code. Run the notebook in Databricks with the assigned project compute cluster (e.g. ITSDA_DAP_TEAM_boostprocessed).
##### 7. Modify the [Boost Agg Staging Pipeline](https://adb-6102124407836814.14.azuredatabricks.net/pipelines/f6ebfe50-a3a3-4e61-92dc-106cc08089c6/updates/ad6c8814-2c92-41f1-b89a-8657a4216449?o=6102124407836814) to run quality checks against your data. 
  - add your three letter country code to the list of countries (or comment the others out and just include yours for speed)
  - Verify that your code passes all quality checks (the graph elements should be all green like below)
  - ![image](https://github.com/user-attachments/assets/731d2d42-35bb-431c-b06c-ca0f91b7949b)
  - resolve quality checks
##### 8. Use the [staging dashboard](https://adb-6102124407836814.14.azuredatabricks.net/dashboardsv3/01f045fc363315389c9c88986ca5fc60/published?o=6102124407836814) to verify that your calculations match those in the Excel workbook.
  - Filter Countries to the newly added country. Check that the year coverage range and number of functional & economic category coverage are as expected.
  - Further filter by Discrepancy $, set min as 1, and investigate discrepancies at various dimensions (e.g., func/econ/total). Resolution may involve an iterative process of updating the pre-existing BOOST Excel workflows and/or updating the ETL pipeline code.
  - ![image](https://github.com/user-attachments/assets/09ab481b-a1c8-4705-aa73-43e7c2ac7ec9)
  - **Our current acceptable threshold of discrepancy is 5%**. 
##### 9. Update Excel calculations (if necessary)
  - If there are formulas in Excel that need to be revised because of double counting, tag them under [Questions for Massimo](https://docs.google.com/document/d/10wdHD5x2IYw6VC-savb2TcC7y1adIPle2dMKOE0D7sI/edit?tab=t.0). See items there for examples.
  - If there needs to be a fix in the BOOST CCI EXCEL sheets, document the content of the changes and leave the logs in the following document: [BOOST CCI changes reconciliations](https://worldbankgroup-my.sharepoint.com/:x:/r/personal/wlu4_worldbank_org/_layouts/15/Doc.aspx?sourcedoc=%7B19BCE6C7-E183-4F11-A41F-E4BBB792ABFC%7D&file=BOOST%20CCI%20changes%20reconciled.xlsx&action=default&mobileredirect=true).
  - Make sure that the EXCEL is updated at both [BOOST's shared project folder](https://worldbankgroup-my.sharepoint.com/:f:/r/personal/mmastruzzi_worldbank_org/Documents/BOOST/Cross-Country%20Interface/Countries?csf=1&web=1&e=eUcv12) and [MEGA project folder for databricks processing](https://worldbankgroup.sharepoint.com/:f:/r/sites/dap/BOOSTProcessed/Shared%20Documents/input/Countries?csf=1&web=1&e=ArX7md). 
  - Any changes to the Excel document should reflect in **Massimo's** source document as well- update his if necessary.
##### 10. Add the new country's ETL pipeline steps to the ["BOOST Harmonize" job](https://adb-6102124407836814.14.azuredatabricks.net/jobs/239032633239734?o=6102124407836814): Workflows > Jobs > BOOST Harmonize > Tasks > + Add task
  - Add the extraction step using Type: Notebook, and Source: git. Use the default cluster as the compute.
  - Add the DLT step using Type: Delta Live Table pipeline, and select your DLT pipeline.
  - Check that the step dependencies are configured correctly.
##### 11. Add the country to the cross-country aggregation DLT pipeline
  - Update [cross_country_aggregate_dlt.py](./cross_country_aggregate_dlt.py) with the country's three letter code to add the table you produce in step 6.
##### 12. Pull request your work
##### 13. Verify your results in the production pipeline
  - Execute the "BOOST Agg" DLT workflow to perform the stacking and aggregation: Workflows > Delta Live Tables > BOOST Agg > Start.
  - Verify your code still passes quality checks.
##### 14. Check the [PowerBI report](https://app.powerbi.com/groups/75fff923-5acd-443e-877b-d2c6e88cdb31/reports/a28af24a-6a8a-4241-bd42-40a4c4af5716/) to ensure the new country is reflected and its narratives are correctly presented.

## boost_gold

| Column Name | Data Type | Description                                                                 | Possible Values                       | Possible Value Catalog Query           |
|-------------|-----------|-----------------------------------------------------------------------------|---------------------------------------|----------------------------------------|
| country_name| string    | The name of the country for which the budget data is recorded               | e.g., "Kenya", "Brazil". See [wbgapi](https://pypi.org/project/wbgapi/) or [WB website](https://data.worldbank.org/country) for a full list | `SELECT DISTINCT country_name FROM indicator.country` |
| year        | int       | The fiscal year for the budget data                                         | e.g., 2023, 2024                      | |
| admin0      | string    | Who spent the money at the highest administrative level                     | Either "Central" or "Regional"        | |
| admin1      | string    | Who spent the money at the first sub-national administrative level          | If admin0 is "Regional," this is the state or province name; if admin0 is "Central," this is always "Central Scope." "Scope" is postfixed to avoid conflicting with a subnational region named "Central" in some countries | `SELECT DISTINCT admin1_region FROM indicator.admin1_boundaries_gold` |
| admin2      | string    | Who spent the money at the second sub-national administrative level         | For "Central" spending, this is the government agency/ministry name; for "Regional" spending, this can be the district or municipality name or subnational government agency/department name | |
| geo0        | string    | Is the money geographically or centrally allocated (either "Central" or "Regional") regardless of the spender | Either "Central" or "Regional" |
| geo1        | string    | Geographically, at which first sub-national administrative level the money was spent | Same as admin1 possible values | `SELECT DISTINCT admin1_region FROM indicator.admin1_boundaries_gold` |
| func        | string    | Functional classification of the budget                                     | e.g., Health, Education. See [Classification of the Functions of Government (COFOG)](https://en.wikipedia.org/wiki/Classification_of_the_Functions_of_Government) for a full list | `SELECT DISTINCT func FROM boost_intermediate.quality_functional_gold` |
| func_sub    | string    | Sub-functional classification under the main COFOG function                 | e.g., "primary education", "secondary education" | |
| econ        | string    | Economic classification of the budget                                       | e.g., "Wage bill", "Goods and services" | `SELECT DISTINCT econ FROM boost_intermediate.quality_economic_gold` |
| econ_sub    | string    | Sub-economic classification under the main economic category                | e.g., "basic wages", "allowances" | |
| approved    | double    | Amount of budget in current local currency approved by the relevant authority | Numeric value representing local currency | |
| revised     | double    | Revised budget in current local currency amount during the fiscal year      | Numeric value representing local currency | |
| executed    | double    | Actual amount spent in current local currency by the end of the fiscal year | Numeric value representing local currency | |

