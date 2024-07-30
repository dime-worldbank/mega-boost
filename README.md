# mega-boost

ELT pipelines that take BOOST coded microdata for each country, clean, harmonize, then store them in a single Delta table. Aggregated budget and expenditure data at the following levels are also created for [visualization in PowerBI](https://app.powerbi.com/groups/627247ef-7122-4351-95f1-86ecacb49b62/reports/c5e8859a-8737-44f5-b950-9be40608e76d/ReportSection?action=OpenReport&pbi_source=ChatInTeams&bookmarkGuid=c6b50c1f-fead-4f60-97ad-920bd128d676):
- by year by country 
- by year by country by adm1

## How to add a new country
- Create a new folder with the country name (Add underscores for countries with 2 words:  Burkina_Faso) 
- Code: You will need
  - 1 notebook for extracting the Boost/raw data from the excel sheet (e.g. ALB_extract_microdata_excel_to_csv.py)
  - 1 notebook for transforming the Boost country data to harmonize with other countries. (e.g. ALB_transform_load_dlt.py).
     - Make sure that the output tables has the country prefix code.
      https://github.com/dime-worldbank/mega-boost/blob/df095567afb187ae6db60181ce81588533bfdd0a/Albania/ALB_transform_load_dlt.py#L128
      This should be consistent with the country code listed in https://github.com/dime-worldbank/mega-boost/blob/df095567afb187ae6db60181ce81588533bfdd0a/cross_country_aggregate_dlt.py#L7
- Test the ELT scripts in databricks (set up DLT workflow if necessary)
  - There are multiple ways to set up the DLT workflow. One way is to first run the notebook with the default cluster (DAP-PROD-CLUSTER). Then create a pipeline following the UI (see below)
  - The pipeline name (anything you like) and target schema name (e.g. boost_intermediate) should be populated 
    ![image](https://github.com/user-attachments/assets/98053ff1-f8ca-49c9-8d93-af437296ffc6)

- Add the script/DLT workflow as steps to the "BOOST Harmonize" Job (Workflows > Jobs)
- Add ELT pipeline for the country's adm1 level subnational population to [mega-indicator repo](https://github.com/weilu/mega-indicators/tree/main/population) – because it's a public good by itself.
  - Can skip this step if the Boost country data does not have subnational data points.
- Add the intermediate table (containing only new country's cleaned microdata) as a source for stacking to [cross_country_aggregate_dlt.py](cross_country_aggregate_dlt.py)
  - Add the country code to https://github.com/dime-worldbank/mega-boost/blob/df095567afb187ae6db60181ce81588533bfdd0a/cross_country_aggregate_dlt.py#L7
  - The country codes are ISO Alpha 3 codes from https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
- Check the [PowerBI report](https://app.powerbi.com/groups/627247ef-7122-4351-95f1-86ecacb49b62/reports/c5e8859a-8737-44f5-b950-9be40608e76d/ReportSection?action=OpenReport&pbi_source=ChatInTeams&bookmarkGuid=c6b50c1f-fead-4f60-97ad-920bd128d676) to ensure the new country is reflected.
- If not all its adm1 areas are present in the map, check adm1_name are aligned in the subnational population dataset and the BOOST dataset.
- Check that the by year by country expenditure matches the aggregated data in BOOST source file
  - You can do this by checking the [MEGA data quality dashboard](https://adb-6102124407836814.14.azuredatabricks.net/dashboardsv3/01ef07bf07d615bb98b5ff5b37e6fa69/published?o=6102124407836814&f_01ef0e080e7416d0a4b27ff8cfa90afc=_all_) .
