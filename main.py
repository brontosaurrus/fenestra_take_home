from sql_handler import SQLHandler
from environment import DB_URL, GCS_BUCKET_NAME, GCS_PROJECT_ID, PSQL_USER, PSQL_PASSWORD, PSQL_IP, PSQL_PORT, PSQL_DB
from google.cloud import storage
import psycopg2
import gzip
import csv
import pandas as pd
import concurrent.futures
import os
from sqlalchemy import create_engine
import json
from multiprocessing import Pool

class GCSHandler:
    def __init__(self, project_id, bucket_name, destination_dir):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.destination_dir = destination_dir
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.get_bucket(bucket_name)

    def list_objects(self):
        blobs = list(self.bucket.list_blobs())
        return [blob.name for blob in blobs]

    def download_object(self, object_name):
        blob = self.bucket.blob(object_name)
        destination = os.path.join(self.destination_dir, os.path.basename(object_name))
        blob.download_to_filename(destination)
        return destination

    def download_new_objects(self, handler):
        objects = self.list_objects()
        # Check if the file has already ben processed by checking the posgresql table created processed_files
        new_objects = [obj for obj in objects if not handler.filename_exists(os.path.basename(obj))]
        new_object_paths = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_object = {executor.submit(self.download_object, obj): obj for obj in new_objects}
            for future in concurrent.futures.as_completed(future_to_object):
                obj = future_to_object[future]
                try:
                    result = future.result()
                    obj_name, result_path = result[0], result[1]
                    new_object_paths.append(os.path.basename(obj))
                    print(f"File {os.path.basename(obj)} downloaded.")
                except Exception as e:
                    print(f"Error downloading object {obj}: {e}")
        return new_object_paths


class DataProcessor:
    def __init__(self, directory, new_files):
        self.directory = directory
        self.new_files = new_files

    def process_file(self, file_name):
        file_path = os.path.join(self.directory, file_name)
        if file_name.endswith('.csv.gz'):
            with gzip.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
        elif file_name.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_name.endswith('.json'):
            with open(file_path, 'r') as f:
                data = json.load(f)
                df = pd.DataFrame(data)
        else:
            print(f"Invalid file type: {file_name}")
            return None
        if df is not None:
            # perform transformation of dataframe and dropping uncessesary columns
            # using a subset of the df for testing
            df = df.head(1000)
            df['Time'] = pd.to_datetime(df['Time'])
            bool_cols = ['MobileDevice', 'IsCompanion', 'ActiveViewEligibleImpression', 'MobileCapability', 'IsInterstitial', 'Anonymous']
            df[bool_cols] = df[bool_cols].astype(bool)
            df.columns = df.columns.str.lower()
            columns_to_drop = ['domain', 'audiencesegmentids', 'publisherprovidedid']
            df.drop(columns=columns_to_drop, inplace=True)
            return df
        else:
            return None
    
    def process_files(self):
        with Pool() as pool:
            # pool to process files in parallel 
            dfs = pool.map(self.process_file, self.new_files)
        return [df for df in dfs if df is not None]
        
    def check_for_duplicates(self,new_df, historic_df):
        concatenated_df = pd.concat([new_df, historic_df], ignore_index=True)
        df_sorted = concatenated_df.sort_values(by=["time"], ascending=False)
        deduplicated_df = df_sorted.drop_duplicates(subset=["orderid", "lineitemid", "keypart"])
        num_duplicates_dropped = len(df_sorted) - len(deduplicated_df)
        return deduplicated_df,num_duplicates_dropped

if __name__ == "__main__":
    
    # -- initialise Google cloud handler and list bucket objcts
    destination_dir = 'downloaded' 
    handler = SQLHandler(dbname=PSQL_DB, user=PSQL_USER, password=PSQL_PASSWORD, host = PSQL_IP, port = PSQL_PORT)
    handler.create_table('processed_files', 'sql/processed_files.sql')
    
    gcs_handler = GCSHandler( GCS_PROJECT_ID, GCS_BUCKET_NAME, destination_dir)
    objects = gcs_handler.list_objects()
    print("Objects in the bucket:", objects)
    
    # -- find new files in bucket and add to processed_files schema 
    os.makedirs(destination_dir, exist_ok=True) 
    new_files = gcs_handler.download_new_objects(handler)
    for obj_path in new_files:
        filename = os.path.basename(obj_path)
        handler.insert_filename( filename)
    
    # -- initialise data processor handler, transform and combine data into df
    processor = DataProcessor(destination_dir, new_files)
    dfs = processor.process_files()
    # check dfs is not empty as there may be no new files pulled
    if len(dfs) > 0:
        new_df = pd.concat(dfs, ignore_index=True)
    else:
        print("No new files to pull")
        sys.exit()
    
    # -- de duplication on basis of rank for three columns and timestamp descending
    handler.create_table('ad_data', 'sql/table_definition.sql')
    historic_df = handler.read_table('ad_data')
    deduplicated_df,num_duplicates_dropped = processor.check_for_duplicates(new_df, historic_df)
    print("Number of duplicates dropped:", num_duplicates_dropped)
    
    # -- use tmp table to only have de duplicated data and swap tables
    handler.create_table('tmp', 'sql/table_definition.sql')
    handler.write_table_chunksize(deduplicated_df, 'tmp', 1000)
    handler.execute_query('ALTER TABLE ad_data RENAME TO tmp_table')
    handler.execute_query('ALTER TABLE tmp RENAME TO ad_data')
    handler.execute_query('DROP TABLE tmp_table')
    
    
    # -- How many records are there per day and per hour?
    results = handler.execute_sql_file('sql/query_1.sql')
    columns = ['day', 'hour', 'record_count']
    df = pd.DataFrame(results, columns=columns)
    print(df)
    
    # -- b. What is the total of the EstimatedBackFillRevenue field per day and per hour
    results = handler.execute_sql_file('sql/query_2.sql')
    columns = ['date', 'hour', 'total_backfill_revenue']
    df = pd.DataFrame(results, columns=columns)
    print(df)
    
     # -- c. How many records and what is the total of the EstimatedBackFillRevenue field per Buyer?
    results = handler.execute_sql_file('sql/query_3.sql')
    columns = ['buyer', 'record_count', 'total_revenue']
    df = pd.DataFrame(results, columns=columns)
    print(df)
    
     # -- d. List the unique Device Categories by Advertiser.
    results = handler.execute_sql_file('sql/query_4.sql')
    columns = ['advertiser', 'device_category']
    df = pd.DataFrame(results, columns=columns)
    print(df)



