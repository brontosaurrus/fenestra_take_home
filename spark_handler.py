from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType
import os
import pandas as pd


class SparkDataProcessor:
    def __init__(self, directory, new_files):
        self.directory = directory
        self.new_files = new_files
        self.spark = SparkSession.builder \
            .appName("DataProcessor") \
            .config("spark.jars", "postgresql-42.7.1.jar") \
            .getOrCreate()

    def process_file(self, file_name):
        file_path = os.path.join(self.directory, file_name)
        if file_name.endswith('.csv.gz'):
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        elif file_name.endswith('.csv'):
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        elif file_name.endswith('.json'):
            df = self.spark.read.json(file_path)
        else:
            print(f"Invalid file type: {file_name}")
            return None
        
        if df is not None:
            df = df.withColumn('Time', col('Time').cast('timestamp'))
            bool_cols = ['MobileDevice', 'IsCompanion', 'ActiveViewEligibleImpression', 'MobileCapability', 'IsInterstitial', 'Anonymous']
            for col_name in bool_cols:
                df = df.withColumn(col_name, col(col_name).cast('boolean'))
            df = df.toDF(*(c.lower() for c in df.columns))
            columns_to_drop = ['domain', 'audiencesegmentids', 'publisherprovidedid']
            df = df.drop(*columns_to_drop)
            return df
        else:
            return None

    def check_for_duplicates(self, new_df, historic_df):
        concatenated_df = new_df.unionByName(historic_df)
        df_sorted = concatenated_df.orderBy("time", ascending=False)
        deduplicated_df = df_sorted.dropDuplicates(["orderid", "lineitemid", "keypart"])
        num_duplicates_dropped = concatenated_df.count() - deduplicated_df.count()
        return deduplicated_df, num_duplicates_dropped
    
    def write_table_chunksize(self, df, table_name, chunk_size):

      jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"
      jdbc_properties = {
          "user": self.user,
          "password": self.password,
          "driver": "org.postgresql.Driver"
      }
      
      df.write.format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", table_name) \
          .option("user", self.user) \
          .option("password", self.password) \
          .option("driver", "org.postgresql.Driver") \
          .option("numPartitions", chunk_size) \
          .mode("append") \
          .save()


    def close(self):
        self.spark.stop()

# test scipts
new_files = ['imps6.csv.gz','imps0.csv.gz' ,'imps5.csv.gz','imps4.csv.gz','imps3.csv.gz','imps2.csv.gz','imps1.csv.gz']
processor = SparkDataProcessor('downloaded', new_files)
dfs = [processor.process_file(file_name) for file_name in new_files if processor.process_file(file_name) is not None]
new_df = dfs[0]
for df in dfs[1:]:
    new_df = new_df.unionByName(df)

# retrieve historic table 
deduplicated_df, num_duplicates_dropped = processor.check_for_duplicates(new_df, historic_df)
# functionality to write table
processor.write_table_chunksize(deduplicated_df, 'tmp', 1000)

processor.close()
