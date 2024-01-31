import os

DB_URL = os.getenv('DB_URL')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'fst-python-case-study')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID', 'fenestra-task-412510')
PSQL_DB = os.getenv('PSQL_DB')
PSQL_USER = os.getenv('PSQL_USER')
PSQL_PASSWORD = os.getenv('PSQL_PASSWORD')
PSQL_IP = os.getenv('PSQL_IP')
PSQL_PORT = os.getenv('PSQL_PORT')