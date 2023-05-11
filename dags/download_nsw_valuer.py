import os
import tempfile
import zipfile
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, MetaData, delete

SOURCE_FILENAME_COLUMN = 'source_filename'

DB_URI = 'postgresql://datadb:datadb@datadb:5432/datadb'
TABLE_NAME = 'nsw_valuer'

DOWNLOAD_FILENAME = 'nsw_valuer.zip'
EXTRACT_DIR_NAME = 'extracted'


def nsw_valuer_http_to_db(db_uri, execution_date):
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory {temp_dir}...")
        download_filename = os.path.join(temp_dir, DOWNLOAD_FILENAME)
        _download_zipfile(download_filename, execution_date)

        extract_path = os.path.join(temp_dir, EXTRACT_DIR_NAME)
        _extract_zipfile(download_filename, extract_path)
        _load_to_db(temp_dir, db_uri, TABLE_NAME)


def _download_zipfile(download_filename, execution_date):
    # url = 'https://www.valuergeneral.nsw.gov.au/__psi/yearly/2022.zip'
    url = f'https://www.valuergeneral.nsw.gov.au/land_value_summaries/lvfiles/LV_{execution_date}.zip'
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
        for filename in files:
            print(f"Found file {filename}...")
            if filename.endswith('.csv'):
                try:
                    file_path = os.path.join(root, filename)
                    print(f"Reading file at {file_path} into pandas df...")
                    df = pd.read_csv(file_path)
                    print(f"Reading file at {file_path} into pandas df... Done")

                    df[SOURCE_FILENAME_COLUMN] = filename

                    df.columns = df.columns.str.lower()
                    new_names = {col: col.replace(' ', '_') for col in df.columns}
                    df = df.rename(columns=new_names)

                    create_table(engine, table_name, df)

                    delete_from_db(engine, table_name, filename)

                    with engine.connect() as connection:
                        print(f"Writing data to db: {db_uri} table: {table_name}...")
                        df.to_sql(table_name, engine, if_exists='append', index=False)
                        print(f"Writing data to db: {db_uri} table: {table_name}... {len(df)} rows. Done")
                except Exception as e:
                    print(f"Error {e}")


def create_table(engine, table_name, df):
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)

        if table_name in metadata.tables:
            print(f"The table {table_name} exists.")
            return

        create_sql = pd.io.sql.get_schema(df.reset_index(), table_name)
        with engine.connect() as conn:
            conn.execute(create_sql)
    except Exception as e:
        print(f"Exception while creating table (it probably exists) {e}")


def delete_from_db(engine, table_name, source_filename):
    try:
        print(f"Deleting from {table_name} WHERE {SOURCE_FILENAME_COLUMN} = {source_filename} ...")

        metadata = MetaData()
        metadata.reflect(bind=engine)
        table = metadata.tables[table_name]

        delete_query = delete(table).where(table.c[SOURCE_FILENAME_COLUMN] == str(source_filename))
        with engine.connect() as conn:
            result = conn.execute(delete_query)
            print(f"Deleted {result.rowcount} rows from the table.")

        print(f"Deleting from {table_name} WHERE {SOURCE_FILENAME_COLUMN} = {source_filename} ... Done")
        print(f"Deleted {result.rowcount} row(s) WHERE {SOURCE_FILENAME_COLUMN} = {source_filename}")
    except Exception as e:
        print(f"Exception while deleting data for {source_filename}")


default_args = {
    'owner': 'sahaj',
    'depends_on_past': False,
}

with DAG(
        'download_nsw_valuer',
        default_args=default_args,
        description='Download NSW land valuer data',
        start_date=datetime(2023, 1, 1),
        schedule='0 0 1 * *',  # Run on the first day of every month
        catchup=True,
) as dag:

    @task
    def nsw_valuer_load(execution_date):
        nsw_valuer_http_to_db(DB_URI, execution_date)

    nsw_valuer_load(execution_date="{{ ds_nodash }}")
