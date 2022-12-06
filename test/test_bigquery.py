from datetime import datetime, timedelta
from unittest import mock

import pandas as pd
import pycarol


def test_is_expired1():
    """BQ.is_expired should return False if self._provided_sa is True."""
    bq_mock = mock.MagicMock()
    bq_mock._provided_sa = True
    assert pycarol.bigquery.BQ.is_expired(bq_mock) is False


@mock.patch("pycarol.bigquery.BQ.token", new_callable=mock.PropertyMock)
def test_is_expired2(mock_token):
    """BQ.is_expired should return True when BQ.token is None."""
    bq_mock = mock.MagicMock()
    mock_token.return_value = None
    assert pycarol.bigquery.BQ.is_expired(bq_mock) is True


@mock.patch("pycarol.bigquery.BQ.token", new_callable=mock.PropertyMock)
def test_is_expired3(mock_token):
    """BQ.is_expired should return True when token has expired."""
    bq_mock = mock.MagicMock()
    offset_date = datetime.now() - timedelta(days=1)
    mock_token.return_value = {"expiration_time": offset_date}
    assert pycarol.bigquery.BQ.is_expired(bq_mock) is True


@mock.patch("pycarol.bigquery.BQ.token", new_callable=mock.PropertyMock)
def test_is_expired4(mock_token):
    """
    BQ.is_expired should return True when token tenant id is different than self
    tenant id.
    """
    bq_mock = mock.MagicMock()
    offset_date = datetime.now() + timedelta(days=1)
    mock_token.return_value = {"expiration_time": offset_date, "env": {"env_id": 5}}
    assert pycarol.bigquery.BQ.is_expired(bq_mock) is True


@mock.patch("pycarol.bigquery.BQ.token", new_callable=mock.PropertyMock)
def test_is_expired5(mock_token):
    """BQ.is_expired should return False if not expired."""
    bq_mock = mock.MagicMock()
    offset_date = datetime.now() + timedelta(days=1)
    mock_token.return_value = {"expiration_time": offset_date, "env": {"env_id": 5}}
    bq_mock.env = {"env_id": 5}
    assert pycarol.bigquery.BQ.is_expired(bq_mock) is False


@mock.patch("pycarol.bigquery._save_local_cache")
@mock.patch("pycarol.bigquery._save_cds_cache")
@mock.patch("pycarol.bigquery._get_tmp_key", return_value={"sa": "sa"})
def test_get_credential1(mock_tmpkey, mock_savecds, mock_savelocal):
    """BQ.get_credential should return credential if expired and save it."""
    bq_mock = mock.MagicMock()
    bq_mock.cached_cds = True
    bq_mock.is_expired = lambda: True
    ret = pycarol.bigquery.BQ.get_credential(bq_mock)
    assert ret["sa"] == mock_tmpkey.return_value["sa"]
    assert "expiration_time" in ret
    assert mock_savelocal.call_count == 1
    assert mock_savecds.call_count == 1


@mock.patch("pycarol.bigquery.BQ.token", new_callable=mock.PropertyMock)
def test_get_credential2(mock_token):
    """BQ.get_credential should return credential if expired and save it."""
    mock_token.return_value = {"sa": 5}
    bq_mock = mock.MagicMock()
    bq_mock.is_expired = lambda: False
    bq_mock.service_account = None
    ret = pycarol.bigquery.BQ.get_credential(bq_mock)
    assert ret == 5


def test_get_fetch_cache1():
    "BQ._fetch_cache should do nothing if token not expired."
    bq_mock = mock.MagicMock()
    bq_mock.is_expired = lambda: False
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == 5
    assert pycarol.bigquery.BQ.token == 5


@mock.patch("pycarol.bigquery._format_sa")
@mock.patch("pycarol.bigquery._load_local_cache")
def test_get_fetch_cache2(mock_load, mock_format):
    "BQ._fetch_cache should get new token if token is expired."
    bq_mock = mock.MagicMock()
    date = datetime.utcnow() + timedelta(days=1)
    mock_format.return_value = {
        "sa": "sa",
        "expiration_time": date,
    }
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: True
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == "sa"
    assert pycarol.bigquery.BQ.token == mock_format.return_value


@mock.patch("pycarol.bigquery._format_sa")
@mock.patch("pycarol.bigquery._load_local_cache")
def test_get_fetch_cache3(mock_load, mock_format):
    "BQ._fetch_cache should do nothing if tmp_file exists and has not expired."
    bq_mock = mock.MagicMock()
    date = datetime.utcnow()
    mock_format.return_value = {
        "sa": "sa",
        "expiration_time": date,
    }
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: True
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == 5
    assert pycarol.bigquery.BQ.token == 5


def test_get_fetch_cache4():
    "BQ._fetch_cache should do nothing if cached_cds is False and token is not expired."
    bq_mock = mock.MagicMock()
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: False
    bq_mock.cache_cds = False
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == 5
    assert pycarol.bigquery.BQ.token == 5


@mock.patch("pycarol.bigquery.Storage")
def test_get_fetch_cache5(mock_storage):
    "BQ._fetch_cache should do nothing if cached token has not expired."
    bq_mock = mock.MagicMock()
    mock_storage.return_value.exists = lambda *args, **kwargs: False
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: False
    bq_mock.cache_cds = True
    bq_mock.storage = None
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == 5
    assert pycarol.bigquery.BQ.token == 5


@mock.patch("pycarol.bigquery.Storage")
def test_get_fetch_cache6(mock_storage):
    "BQ._fetch_cache should do nothing if storage.exists raises exception."
    bq_mock = mock.MagicMock()

    def raise_(*args, **kwargs):
        raise Exception

    mock_storage.return_value.exists = raise_
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: False
    bq_mock.cache_cds = True
    bq_mock.storage = None
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == 5
    assert pycarol.bigquery.BQ.token == 5


@mock.patch("pycarol.bigquery._format_sa")
@mock.patch("pycarol.bigquery._load_local_cache")
@mock.patch("pycarol.bigquery.Storage")
def test_get_fetch_cache7(mock_storage, mock_load, mock_format):
    "BQ._fetch_cache should do nothing if cached token has not expired."
    bq_mock = mock.MagicMock()
    mock_storage.return_value.exists = lambda *args, **kwargs: True
    mock_storage.return_value.load = lambda *args, **kwargs: "/tmp/bla"
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: False
    bq_mock.cache_cds = True
    bq_mock.storage = None
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    date = datetime.utcnow()
    mock_format.return_value = {
        "sa": "sa",
        "expiration_time": date,
    }
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == 5
    assert pycarol.bigquery.BQ.token == 5


@mock.patch("pycarol.bigquery._format_sa")
@mock.patch("pycarol.bigquery._load_local_cache")
@mock.patch("pycarol.bigquery.Storage")
def test_get_fetch_cache8(mock_storage, mock_load, mock_format):
    "BQ._fetch_cache should load token if expired."
    bq_mock = mock.MagicMock()
    mock_storage.return_value.exists = lambda *args, **kwargs: True
    mock_storage.return_value.load = lambda *args, **kwargs: "/tmp/bla"
    bq_mock.is_expired = lambda: True
    bq_mock._temp_file_path.exists = lambda: False
    bq_mock.cache_cds = True
    bq_mock.storage = None
    bq_mock.service_account = 5
    pycarol.bigquery.BQ.token = 5
    date = datetime.utcnow() + timedelta(days=1)
    mock_format.return_value = {
        "sa": "sa",
        "expiration_time": date,
    }
    assert pycarol.bigquery.BQ._fetch_cache(bq_mock) is None
    assert bq_mock.service_account == "sa"
    assert pycarol.bigquery.BQ.token == mock_format.return_value


def test_get_tmp_key() -> None:
    "BQ._get_tmp_key should return whatever it gets from Carol API."
    carol = mock.MagicMock()
    carol.call_api = lambda *args, **kwargs: {"x": "x"}
    ret = pycarol.bigquery._get_tmp_key("exp_time", carol)
    assert ret == {"x": "x"}


@mock.patch("pycarol.bigquery.BQ.token")
@mock.patch("pycarol.bigquery._generate_client")
def test_query(mock_genclient, mock_bqtoken) -> None:
    "BQ.query should return query results."
    bq_mock = mock.MagicMock()
    ret_val = [{"col1": "col1", "col2": "col2"}]
    mock_genclient.return_value.query.return_value = ret_val

    ret = pycarol.bigquery.BQ.query(bq_mock, "", return_dataframe=False)
    assert ret == ret_val

    ret = pycarol.bigquery.BQ.query(bq_mock, "")
    assert pd.DataFrame(ret_val).equals(ret)
