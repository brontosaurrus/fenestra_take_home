from sql_handler import SQLHandler
from environment import DB_URL, GCS_BUCKET_NAME, GCS_PROJECT_ID, PSQL_USER, PSQL_PASSWORD, PSQL_IP, PSQL_PORT, PSQL_DB
from google.cloud import storage
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

    def download_new_objects(self):
        downloaded_files = os.listdir(self.destination_dir)
        objects = self.list_objects()
        new_objects = [obj for obj in objects if os.path.basename(obj) not in downloaded_files]
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
            df['Time'] = pd.to_datetime(df['Time'])
            bool_cols = ['MobileDevice', 'IsCompanion', 'ActiveViewEligibleImpression', 'MobileCapability', 'IsInterstitial', 'Anonymous']
            df[bool_cols] = df[bool_cols].astype(bool)
            df.columns = df.columns.str.lower()
            columns_to_drop = ['domain', 'audiencesegmentids', 'publisherprovidedid']
            df.drop(columns=columns_to_drop, inplace=True)
            df = df.head(20000)
            return df
        else:
            return None

    def process_files(self):
        with Pool() as pool:
            dfs = pool.map(self.process_file, self.new_files)
        return [df for df in dfs if df is not None]


if __name__ == "__main__":
    
    # -- initialise Google cloud handler and list bucket objcts
    destination_dir = 'downloaded' 
    
    gcs_handler = GCSHandler( GCS_PROJECT_ID, GCS_BUCKET_NAME, destination_dir)
    objects = gcs_handler.list_objects()
    print("Objects in the bucket:", objects)
    
    # -- find new files in bucket 
    os.makedirs(destination_dir, exist_ok=True) 
    new_files = gcs_handler.download_new_objects()
    
    # -- initialise data processor handler, transform and combine data into df
    new_files = ['imps6.csv.gz','imps0.csv.gz' ,'imps5.csv.gz','imps4.csv.gz','imps3.csv.gz','imps2.csv.gz','imps1.csv.gz'] # this wll be taken from above
    processor = DataProcessor(destination_dir, new_files)
    dfs = processor.process_files()
    new_df = pd.concat(dfs, ignore_index=True)

    
    # -- de duplication on basis of rank for three columns and timestamp descending
    handler = SQLHandler(dbname=PSQL_DB, user=PSQL_USER, password=PSQL_PASSWORD, host = PSQL_IP, port = PSQL_PORT)
    handler.create_table('ad_data', 'table_definition.sql')
    historic_df = handler.read_table('ad_data')
    concatenated_df = pd.concat([new_df, historic_df], ignore_index=True)
    df_sorted = concatenated_df.sort_values(by=["time"], ascending=False)
    deduplicated_df = df_sorted.drop_duplicates(subset=["orderid", "lineitemid", "keypart"])
    deduplicated_df.to_csv('output.csv', index=False)
    
    # Check the deduplicated DataFrame
    num_duplicates_dropped = len(df_sorted) - len(deduplicated_df)
    print("Number of duplicates dropped:", num_duplicates_dropped)
    print(len(deduplicated_df))
    
    # -- use tmp table to only have de duplicated data and swap tables
    handler.create_table('tmp', 'table_definition.sql')
    handler.write_table_chunksize(deduplicated_df, 'tmp', 1000)
    handler.execute_query('ALTER TABLE ad_data RENAME TO tmp_table')
    handler.execute_query('ALTER TABLE tmp RENAME TO ad_data')
    handler.execute_query('DROP TABLE tmp_table')

     
    # -- check sql data 
    df = handler.read_table('ad_data')
    print(len(df))



