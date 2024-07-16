# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

def get_cci_metadata(prune=False):
    files = glob(f"{INPUT_DIR}/*.xlsx")
    files.sort(key=os.path.getmtime, reverse=True)

    columns = ["country", "fiscal_year", "data_source", "updated_at"]
    meta_df = pd.DataFrame(columns=columns)

    countries = spark.table('indicator.country').toPandas()
    
    for filename in tqdm(files):
        try:
            for sheet_name in ['Approved', 'Executed']: # quick & dirty way to make sure both sheets exit
                df = pd.read_excel(filename, sheet_name=sheet_name, na_values=['..'])
        except ValueError as e:
            print(f"Error reading {sheet_name} from {filename}")
            print(e)
            continue
        
        meta_columns = {'Country': 'country', 'Fiscal year': 'fiscal_year'}
        country_info = df.loc[:, meta_columns.keys()]\
                         .dropna().drop_duplicates()\
                         .rename(columns=meta_columns)\
                         .iloc[0:1, :]
        assert country_info.shape == (1, 2), f'Unexpected country information {country_info} with shape {country_info.shape} from {filename}'
        
        country_info['data_source'] = filename
        country_info['updated_at'] = os.path.getmtime(filename)
        country_row = country_info.iloc[0]

        # map country code to country name
        if len(country_row.country) == 3 and country_row.country.upper() == country_row.country:
            country_found = countries[countries.country_code == country_row.country]
            if country_found.empty:
                print(f"Unable to look up country code {country_row.country} from {filename}. Skipping")
                continue
            country_info['country_code'] = country_row.country
            country_name = country_found.iloc[0].country_name
            country_info['country'] = country_name
        else:
            print(f"Expect a 3-letter country code in the 'Country' column from {filename}, but found {country_row.country}. Skipping")
            continue

        country_in_meta_df = meta_df[meta_df.country == country_name]
        if not country_in_meta_df.empty:
            existing_country = country_in_meta_df.iloc[0]
            file_basename = os.path.basename(filename)
            existing_file_basename = os.path.basename(existing_country.data_source)
            print(f'Skipping {file_basename} because a more recent version of {country_name} already processed: {existing_file_basename} last modified {existing_country.updated_at}')
            if prune:
                dbutils.fs.rm(filename.replace("/dbfs", ""))
                print(f"Pruned {file_basename}")
            continue

        meta_df = pd.concat([meta_df, country_info], ignore_index=True)
        
    return meta_df

# COMMAND ----------

metadata_table_name = "boost_intermediate.boost_country_metadata"
if spark.catalog.tableExists(metadata_table_name):
    existing_metadata_df = spark.table(metadata_table_name).toPandas()
else:
    existing_metadata_df = get_cci_metadata()
    # artificially make the updated at older than the actual value so all countries will be processed
    existing_metadata_df['updated_at'] = existing_metadata_df['updated_at'] - 1e+09

metadata_df = get_cci_metadata()
merged_metadata_df = pd.merge(metadata_df, existing_metadata_df, on=['country_code'], how='left', suffixes=('', '_old'))

# handling for the new countries added
existing_countries = existing_metadata_df.country.unique()
current_countries = metadata_df.country.unique()
new_countries = [country for country in current_countries if country not in existing_countries]
for country in new_countries:
    merged_metadata_df.loc[merged_metadata_df.country == country, 'updated_at_old'] =  existing_metadata_df.loc[merged_metadata_df.country == country,'updated_at'] - 1e+09

merged_metadata_df

# COMMAND ----------

to_process_df = merged_metadata_df[merged_metadata_df.updated_at > merged_metadata_df.updated_at_old]
display(to_process_df)

# COMMAND ----------

tqdm.pandas()

def process_country(meta_row):
    filename = meta_row.data_source
    filename_stem = Path(filename).stem
    if 'non boost' in filename_stem:
        print(f"{filename_stem}.xlsx is non boost. Skipping")
        return
    
    csv_dir = f"{WORKSPACE_DIR}/cci_csv/{meta_row.country_code}"
    Path(csv_dir).mkdir(parents=True, exist_ok=True)
    
    for sheet_name in ['Approved', 'Executed']:
        df = pd.read_excel(filename, sheet_name=sheet_name, na_values=['..'])
        
        first_year_col = next(col for col in df.columns if str(col).startswith('2'))
        first_year_col_index = df.columns.get_loc(first_year_col)
        
        for col_index, col_name in enumerate(df.columns[first_year_col_index:], start=first_year_col_index):
            if not str(col_name).startswith('2'):
                last_year_col_index = col_index - 1
                break
  
        category_code_col = df.columns[first_year_col_index-2]
        last_year_col = df.columns[last_year_col_index]
        data = df.loc[:, category_code_col:last_year_col]
        data.columns = ['category_code', 'category'] + list(range(2006, 2006+len(data.columns)-2))
        print(f'{meta_row.country} {sheet_name} {data.columns}')
        csv_filename = f"{csv_dir}/{sheet_name}.csv"
        data.to_csv(csv_filename, index=False)
    return data.shape[0] # only return the number of rows of executed for quick sanity check

to_process_df.progress_apply(process_country, axis=1)

# COMMAND ----------

sdf = spark.createDataFrame(metadata_df)
sdf.write.mode("overwrite").saveAsTable(metadata_table_name)
