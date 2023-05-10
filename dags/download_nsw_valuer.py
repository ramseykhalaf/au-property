import os
import tempfile
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine

DB_URI = 'postgresql://datadb:datadb@datadb:5432/datadb'
TABLE_NAME = 'nsw_valuer'

DOWNLOAD_FILENAME = 'nsw_valuer.zip'
EXTRACT_DIR_NAME = 'extracted'

default_args = {
    'owner': 'sahaj',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2023, 5, 1),
}

with DAG(
        'download_nsw_valuer',
        default_args=default_args,
        description='Download NSW land valuer data',
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:


    @task(task_id = "nsw_valuer_http_to_db")
    def _nsw_valuer_http_to_db():
        with tempfile.TemporaryDirectory() as temp_dir:
            download_filename = os.path.join(temp_dir, DOWNLOAD_FILENAME)
            _download_zipfile(download_filename)

            extract_path = os.path.join(temp_dir, EXTRACT_DIR_NAME)
            _extract_zipfile(download_filename, extract_path)
            _load_to_db(temp_dir, DB_URI, TABLE_NAME)

    def _download_zipfile(download_filename):
        # url = 'https://www.valuergeneral.nsw.gov.au/__psi/yearly/2022.zip'
        url = 'https://www.valuergeneral.nsw.gov.au/land_value_summaries/lvfiles/LV_20230501.zip'
        print(f"Downloading data from {url}...")
        response = requests.get(url)
        print(f"Downloading data from {url}...  Done")
        with open(download_filename, 'wb') as output_file:
            print(f"Writing downloaded data to {download_filename}...")
            output_file.write(response.content)
            print(f"Writing downloaded data to {download_filename}... Done")


    def _extract_zipfile(zip_path, output_dir):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
            for member in zip_ref.infolist():
                if member.filename.endswith('.zip'):
                    nested_zip_path = os.path.join(output_dir, member.filename)
                    nested_output_directory = os.path.join(output_dir, os.path.splitext(member.filename)[0])
                    os.makedirs(nested_output_directory, exist_ok=True)
                    _extract_zipfile(nested_zip_path, nested_output_directory)

    def _load_to_db(directory, db_uri, table_name):
        engine = create_engine(db_uri)
        print(f"Walking directory {directory}...")
        for root, dirs, files in os.walk(directory):
            for file in files:
                print(f"Found file {file}...")
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    print(f"Reading file at {file_path} into pandas df...")
                    df = pd.read_csv(file_path)
                    print(f"Reading file at {file_path} into pandas df... Done")

                    print(f"Writing data to db: {db_uri} table: {table_name}...")
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                    print(f"Writing data to db: {db_uri} table: {table_name}... Done")

    nsw_valuer_http_to_db = _nsw_valuer_http_to_db()

    nsw_valuer_http_to_db
