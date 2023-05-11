import os
import tempfile
from unittest.mock import Mock, patch

import boto3
import pytest
from moto import mock_s3
from sqlalchemy import create_engine

from download_nsw_valuer_v2 import _http_to_s3, _s3_to_db, _clean_db

TEST_DATE = "20230501"
TEST_TABLE_NAME = "test_nsw_valuer"
TEST_S3_BUCKET = "test-s3-bucket"
TEST_S3_PREFIX = "test-s3-prefix"


@pytest.fixture
def sample_content():
    current_file = os.path.realpath(__file__)
    current_directory = os.path.dirname(current_file)
    test_zip_file = os.path.join(current_directory, 'test/data/LV_20230501.zip')
    with open(test_zip_file, 'rb') as test_zip_content:
        yield test_zip_content.read()


@pytest.fixture(autouse=True)
def mock_nsw_valuer_http(sample_content):
    mock_resp = Mock()
    mock_resp.content = sample_content
    with patch('requests.get', return_value=mock_resp):
        yield


@pytest.fixture
def mock_db_uri():
    with tempfile.NamedTemporaryFile() as f:
        yield f'sqlite:///{f.name}'


@pytest.fixture
def mock_s3_resource():
    mock_s3_session = mock_s3()
    mock_s3_session.start()
    s3 = boto3.resource("s3")

    source_bucket = s3.Bucket(TEST_S3_BUCKET)
    source_bucket.create()

    with patch('download_nsw_valuer_v2._get_s3', return_value=s3):
        yield s3

    mock_s3_session.stop()


def test__nsw_valuer_http_to_s3(sample_content, mock_s3_resource):
    _http_to_s3(TEST_S3_BUCKET, TEST_S3_PREFIX, TEST_DATE)

    s3_object = mock_s3_resource.Object(TEST_S3_BUCKET, f'{TEST_S3_PREFIX}/LV_{TEST_DATE}.zip')
    uploaded_content = s3_object.get()['Body'].read()
    assert uploaded_content == sample_content


def test__nsw_valuer_s3_to_db(sample_content, mock_s3_resource, mock_db_uri):
    # Easiest way to setup s3 mock is to use previous pipeline step
    _http_to_s3(TEST_S3_BUCKET, TEST_S3_PREFIX, TEST_DATE)

    _s3_to_db(TEST_S3_BUCKET, TEST_S3_PREFIX, mock_db_uri, TEST_TABLE_NAME, TEST_DATE)

    engine = create_engine(mock_db_uri)
    with engine.connect() as conn:
        result = conn.execute(f"SELECT count(*) FROM {TEST_TABLE_NAME} limit 5")
        rows = result.fetchall()
        assert rows[0][0] == 18


def test__clean_db_for_date(sample_content, mock_s3_resource, mock_db_uri):
    _http_to_s3(TEST_S3_BUCKET, TEST_S3_PREFIX, TEST_DATE)
    _s3_to_db(TEST_S3_BUCKET, TEST_S3_PREFIX, mock_db_uri, TEST_TABLE_NAME, TEST_DATE)

    _clean_db(mock_db_uri, TEST_TABLE_NAME, TEST_DATE)

    _s3_to_db(TEST_S3_BUCKET, TEST_S3_PREFIX, mock_db_uri, TEST_TABLE_NAME, TEST_DATE)

    engine = create_engine(mock_db_uri)
    with engine.connect() as conn:

        result2 = conn.execute(f"SELECT count(*) FROM {TEST_TABLE_NAME} limit 5")
        rows = result2.fetchall()
        assert rows[0][0] == 18


