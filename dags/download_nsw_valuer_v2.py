import os
import tempfile
import zipfile
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, MetaData, delete
import boto3
from airflow.models import Connection


SOURCE_DATE_COLUMN = '_source_date'
SOURCE_FILENAME_COLUMN = '_source_file'

S3_BUCKET = 'sahaj-aus-beach-de-1'
S3_PREFIX = 'data/nsw_valuer/raw/land_value_summaries'

DB_URI = 'postgresql://datadb:datadb@datadb:5432/datadb'
TABLE_NAME = 'nsw_valuer_v2'

DOWNLOAD_FILENAME = 'nsw_valuer.zip'
EXTRACT_DIR_NAME = 'extracted'


def _get_s3():
    aws_conn = Connection.get_connection_from_secrets("aws")
    aws_access_key_id = aws_conn.login
    aws_secret_access_key = aws_conn.password

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    return s3


def _http_to_s3(s3_bucket, s3_prefix, source_date):
    s3 = _get_s3()

    url = f'https://www.valuergeneral.nsw.gov.au/land_value_summaries/lvfiles/LV_{source_date}.zip'
    print(f"Downloading data from {url}...")
    response = requests.get(url)
    print(f"Downloading data from {url}...  Done")

    s3_key = _s3_key(s3_prefix, source_date)

    print(f"Uploading downloaded data to s3_bucket: {s3_bucket}, s3_key: {s3_key} ...")
    s3.Bucket(s3_bucket).upload_fileobj(BytesIO(response.content), s3_key)
    print("Upload Successful")
    print(f"Uploading downloaded data to s3_bucket: {s3_bucket}, s3_key: {s3_key} ... Done")


def _s3_key(s3_prefix, source_date):
    return f'{s3_prefix}/LV_{source_date}.zip'


def _s3_to_db(s3_bucket, s3_prefix, db_uri, table_name, source_date):
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory {temp_dir}...")

        download_filename = os.path.join(temp_dir, DOWNLOAD_FILENAME)
        _s3_to_temp_zipfile(s3_bucket, s3_prefix, download_filename, source_date)

        extract_path = os.path.join(temp_dir, EXTRACT_DIR_NAME)
        _extract_zipfile(download_filename, extract_path)
        _load_to_db(temp_dir, db_uri, table_name, source_date)


def _s3_to_temp_zipfile(s3_bucket, s3_prefix, dest_filename, source_date):
    print(f"Downloading data from s3 bucket: {s3_bucket} s3 prefix: {s3_prefix}...")
    s3 = _get_s3()
    s3_object = s3.Object(s3_bucket, _s3_key(s3_prefix, source_date))
    s3_content = s3_object.get()['Body'].read()
    print(f"Downloading data from s3 bucket: {s3_bucket} s3 prefix: {s3_prefix}... Done")

    with open(dest_filename, 'wb') as f:
        print(f"Writing downloaded data to {dest_filename}...")
        f.write(s3_content)
        print(f"Writing downloaded data to {dest_filename}... Done")


def _extract_zipfile(zip_path, output_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
        for member in zip_ref.infolist():
            if member.filename.endswith('.zip'):
                nested_zip_path = os.path.join(output_dir, member.filename)
                nested_output_directory = os.path.join(output_dir, os.path.splitext(member.filename)[0])
                os.makedirs(nested_output_directory, exist_ok=True)
                _extract_zipfile(nested_zip_path, nested_output_directory)


def _load_to_db(directory, db_uri, table_name, source_date):
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

                    df[SOURCE_DATE_COLUMN] = source_date
                    df[SOURCE_FILENAME_COLUMN] = filename

                    df.columns = df.columns.str.lower()
                    new_names = {col: col.replace(' ', '_') for col in df.columns}
                    df = df.rename(columns=new_names)

                    _create_table(engine, table_name, df)

                    with engine.connect() as connection:
                        print(f"Writing data to db: {db_uri} table: {table_name}...")
                        df.to_sql(table_name, engine, if_exists='append', index=False)
                        print(f"Writing data to db: {db_uri} table: {table_name}... {len(df)} rows. Done")
                except Exception as e:
                    print(f"Error {e}")


def _create_table(engine, table_name, df):
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)

        if table_name in metadata.tables:
            print(f"The table {table_name} exists.")
            return

        create_sql = pd.io.sql.get_schema(df.reset_index(), table_name)
        with engine.connect() as conn:
            print(f"Creating table with SQL: {create_sql} ...")
            conn.execute(create_sql)
            print(f"Creating table with SQL... Done")
    except Exception as e:
        print(f"Exception while creating table (it probably exists) {e}")


def _clean_db(db_uri, table_name, source_date):
    engine = create_engine(db_uri)

    print(f"Deleting from {table_name} where {SOURCE_DATE_COLUMN} = {source_date} ...")

    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]

    delete_query = delete(table).where(table.c[SOURCE_DATE_COLUMN] == str(source_date))
    with engine.connect() as conn:
        result = conn.execute(delete_query)
        print(f"Deleted {result.rowcount} rows from the table.")

    print(f"Deleting from {table_name} where {SOURCE_DATE_COLUMN} = {source_date} ... Done")
    print(f"Deleted {result.rowcount} row(s) where {SOURCE_DATE_COLUMN} = {source_date}")


default_args = {
    'owner': 'sahaj',
    'depends_on_past': False,
}

with DAG(
        'download_nsw_valuer_v2',
        default_args=default_args,
        description='Download NSW land valuer data',
        start_date=datetime(2023, 1, 1),
        schedule='0 0 1 * *',  # Run on the first day of every month
        catchup=True,
) as dag:

    @task
    def http_to_s3(execution_date):
        _http_to_s3(S3_BUCKET, S3_PREFIX, execution_date)

    @task
    def clean_db(execution_date):
        _clean_db(DB_URI, TABLE_NAME, execution_date)

    @task
    def s3_to_db(execution_date):
        _s3_to_db(S3_BUCKET, S3_PREFIX, DB_URI, TABLE_NAME, execution_date)

    http_to_s3(execution_date="{{ ds_nodash }}") \
        >> clean_db(execution_date="{{ ds_nodash }}") \
        >> s3_to_db(execution_date="{{ ds_nodash }}")