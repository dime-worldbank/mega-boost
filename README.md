# mega-boost

ELT pipelines that take BOOST coded microdata for each country, clean, harmonize, then store them in a single Delta table. Aggregated budget and expenditure data at the following levels are also created for [visualization in PowerBI](https://app.powerbi.com/groups/627247ef-7122-4351-95f1-86ecacb49b62/reports/c5e8859a-8737-44f5-b950-9be40608e76d/ReportSection?action=OpenReport&pbi_source=ChatInTeams&bookmarkGuid=c6b50c1f-fead-4f60-97ad-920bd128d676):
- by year by country 
- by year by country by adm1

## How to add a new country
- Create a new folder with the country name in CamelCase
- Code & test the ELT scripts in databricks (set up DLT workflow if necessary)
- Add the script/DLT workflow as steps to the "BOOST Harmonize" Job (Workflows > Jobs)
- Add ELT pipeline for the country's adm1 level subnational population to [mega-indicator repo](https://github.com/weilu/mega-indicators/tree/main/population) – because it's a public good by itself.
- Add the intermediate table (containing only new country's cleaned microdata) as a source for stacking to [cross_country_aggregate_dlt.py](cross_country_aggregate_dlt.py)
- Check the [PowerBI report](https://app.powerbi.com/groups/627247ef-7122-4351-95f1-86ecacb49b62/reports/c5e8859a-8737-44f5-b950-9be40608e76d/ReportSection?action=OpenReport&pbi_source=ChatInTeams&bookmarkGuid=c6b50c1f-fead-4f60-97ad-920bd128d676) to ensure the new country is reflected.
- If not all its adm1 areas are present in the map, check adm1_name are aligned in the subnational population dataset and the BOOST dataset.
- Check that the by year by country expenditure matches the aggregated data in BOOST source file
