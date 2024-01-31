import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

class SQLHandler:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
    
    def execute_query(self, query):
        conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
            print("Query executed successfully!")
        except Exception as e:
            print("Error executing query:", e)
        finally:
            cursor.close()
            conn.close()

    def create_table(self, table_name, file_path):
        with open(file_path, 'r') as file:
            create_table_statement = file.read()
    
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({create_table_statement})"
        self.execute_query(query)


    def read_table(self, table_name):
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
        try:
            df = pd.read_sql_table(table_name, con=engine)
            return df
        except Exception as e:
            print("Error reading table:", e)
        finally:
            engine.dispose()

    def write_table(self, df, table_name):
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
            print("Data successfully written to table:", table_name)
        except Exception as e:
            print("Error writing data to table:", e)
        finally:
            engine.dispose()
            
    def write_table_chunksize(self, df, table_name,chunksize):
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
        try:
            total_rows = len(df)
            num_chunks = (total_rows // chunksize) + 1  

            for i in range(num_chunks):
                start_idx = i * chunksize
                end_idx = min((i + 1) * chunksize, total_rows)

                chunk_df = df.iloc[start_idx:end_idx] 

                chunk_df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

                print(f"Chunk {i+1}/{num_chunks} written successfully to table:", table_name)
            
            print("Data successfully written to table:", table_name)
        except Exception as e:
            print("Error writing data to table:", e)

    