# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

metadata_table_name = "boost_intermediate.boost_country_metadata"
if spark.catalog.tableExists(metadata_table_name):
    existing_metadata_df = spark.table(metadata_table_name).toPandas()
else:
    existing_metadata_df = get_cci_metadata()
    # artificially make the updated at older than the actual value so all countries will be processed
    existing_metadata_df['updated_at'] = existing_metadata_df['updated_at'] - 1e+09

metadata_df = get_cci_metadata()
merged_metadata_df = pd.merge(metadata_df, existing_metadata_df, on=['country'], how='left', suffixes=('', '_old'))
merged_metadata_df

# COMMAND ----------

to_process_df = merged_metadata_df[merged_metadata_df.updated_at > merged_metadata_df.updated_at_old]
to_process_df

# COMMAND ----------

tqdm.pandas()

def process_country(meta_row):
    filename = meta_row.data_source
    filename_stem = Path(filename).stem
    if 'non boost' in filename_stem:
        print(f"{filename_stem}.xlsx is non boost. Skipping")
        return
    
    csv_dir = f"{WORKSPACE_DIR}/cci_csv/{meta_row.country}"
    Path(csv_dir).mkdir(parents=True, exist_ok=True)
    
    for sheet_name in ['Approved', 'Executed']:
        df = pd.read_excel(filename, sheet_name=sheet_name, na_values=['..'])
        unnamed_cols = df.filter(regex='^Unnamed.+', axis=1).columns
        if len(unnamed_cols) < 1:
            unnamed_cols = df.filter(regex='^[\s|\.]+$', axis=1).columns
        assert len(unnamed_cols) > 0, f'Expect to find an unnamed or blank column in {df.columns} from {filename}'
        first_year_col = next(col for col in df.columns if str(col).startswith('200'))
        category_col = df.columns[df.columns.get_loc(first_year_col)-1]
        data = df.loc[:, category_col:unnamed_cols[0]].dropna(axis=1, how="all")
        data.columns = ['category'] + list(range(2006, 2006+len(data.columns)-1))
        csv_filename = f"{csv_dir}/{sheet_name}.csv"
        data.to_csv(csv_filename, index=False)

    return data # only return executed for quick ref

result_df = to_process_df.progress_apply(process_country, axis=1, result_type="expand")
result_df

# COMMAND ----------

sdf = spark.createDataFrame(metadata_df)
sdf.write.mode("overwrite").saveAsTable("boost_intermediate.boost_country_meta")
