import os
import tempfile
from unittest.mock import Mock, patch

from sqlalchemy import create_engine

from download_nsw_valuer import nsw_valuer_http_to_db


def test_nsw_valuer_http_to_db():
    with tempfile.NamedTemporaryFile() as f:
        db_uri = f'sqlite:///{f.name}'

        current_file = os.path.realpath(__file__)
        current_directory = os.path.dirname(current_file)
        test_zip_file = os.path.join(current_directory, 'test/data/LV_20230501.zip')

        with open(test_zip_file, 'rb') as sample_zip_content:

            mock_resp = Mock()
            mock_resp.content = sample_zip_content.read()
            with patch('requests.get', return_value=mock_resp):

                nsw_valuer_http_to_db(db_uri, "20230501")
                nsw_valuer_http_to_db(db_uri, "20230501")

                engine = create_engine(db_uri)
                with engine.connect() as conn:
                    result = conn.execute("SELECT count(*) FROM nsw_valuer limit 5")
                    rows = result.fetchall()

                    assert rows[0][0] == 18
