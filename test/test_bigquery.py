from datetime import datetime, timedelta
import json
from pathlib import Path
from unittest import mock
import typing as T

import pycarol


def test_token_init() -> None:
    """Test the initialization of the Token class in the pycarol.bigquery module."""
    token_mock = mock.MagicMock()
    service_account = {"expiration_time": "expiration_time"}
    env: T.Dict = {}
    pycarol.bigquery.Token.__init__(token_mock, service_account, env)
    assert token_mock._env == env
    assert token_mock.expiration_time == service_account["expiration_time"]
    assert token_mock.service_account == service_account


def test_token_to_dict() -> None:
    """Test the to_dict() method of the Token class in the pycarol.bigquery module."""
    token_mock = mock.MagicMock()
    token_mock.service_account = {}
    token_mock._env = {}
    token_mock.expiration_time = ""
    _dict = pycarol.bigquery.Token.to_dict(token_mock)
    assert _dict == {"service_account": {}, "env": {}, "expiration_time": ""}


def test_token_expired() -> None:
    """Test the expired() method of the Token class in the pycarol.bigquery module."""
    token_mock = mock.MagicMock()
    dt_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    pass_date = datetime.utcnow() - timedelta(1)
    token_mock.expiration_time = datetime.strftime(pass_date, dt_format)
    expired = pycarol.bigquery.Token.expired(token_mock)
    assert expired is True


@mock.patch("pycarol.bigquery.Storage")
def test_token_manager_init(storage_mock) -> None:
    """Test the initialization of the pycarol.bigquery.TokenManager class."""
    manager_mock = mock.MagicMock()
    carol_mock = mock.MagicMock()
    carol_mock.get_current.return_value = {"env_id": 5}
    pycarol.bigquery.TokenManager.__init__(manager_mock, carol_mock)
    assert manager_mock._carol == carol_mock
    assert manager_mock.expiration_window == 24
    assert manager_mock._env == {"env_id": 5}
    base_dir = Path(pycarol.__TEMP_STORAGE__)
    assert manager_mock._tmp_filepath == base_dir / ".pycarol_temp_5.json"
    assert manager_mock.cache_cds is True
    assert manager_mock._storage == storage_mock.return_value
    assert manager_mock.token == manager_mock.get_token.return_value


def test_token_manager_issue_new_key() -> None:
    """Test the _issue_new_key() method of the pycarol.bigquery.TokenManager class."""
    manager_mock = mock.MagicMock()
    manager_mock._carol = mock.MagicMock()
    manager_mock._carol.call_api.return_value = {}
    key = pycarol.bigquery.TokenManager._issue_new_key(manager_mock)
    assert key == {}


def test_token_manager_save_token_file() -> None:
    """Test the _save_token_file() method of the pycarol.bigquery.TokenManager class."""
    manager_mock = mock.MagicMock()
    manager_mock._tmp_filepath = Path("/tmp/pycarol_test/test_sa.env")
    token_mock = mock.MagicMock()
    token_mock.to_dict.return_value = {"test": "test"}
    pycarol.bigquery.TokenManager._save_token_file(manager_mock, token_mock)
    with open(manager_mock._tmp_filepath, "r", encoding="utf-8") as file:
        sa = json.load(file)
    assert sa == {"test": "test"}

    Path("/tmp/pycarol_test/test_sa.env").unlink()


def test_token_manager_save_token_cloud() -> None:
    """Test the _save_token_cloud() method of the pycarol.bigquery.TokenManager."""
    manager_mock = mock.MagicMock()
    Path("/tmp/pycarol_test/test_sa.env").touch()
    pycarol.bigquery.TokenManager._save_token_cloud(manager_mock)
    assert manager_mock._storage.save.called is True
    Path("/tmp/pycarol_test/test_sa.env").unlink()


@mock.patch("pycarol.bigquery.Token")
def test_token_manager_load_token_file(token_mock) -> None:
    """Test the _load_token_file() method of the pycarol.bigquery.TokenManager class."""
    sa = {"service_account": "test", "env": "test"}
    test_path = Path("/tmp/pycarol_test/test_sa.env")
    with open(test_path, "w", encoding="utf-8") as file:
        json.dump(sa, file)

    manager_mock = mock.MagicMock()
    manager_mock._tmp_filepath = test_path
    token = pycarol.bigquery.TokenManager._load_token_file(manager_mock)
    assert token == token_mock.return_value
    test_path.unlink()


def test_token_manager_load_token_cloud() -> None:
    """Test the _load_token_cloud() method of the pycarol.bigquery.TokenManager."""
    manager_mock = mock.MagicMock()
    test_path = Path("/tmp/pycarol_test/test_sa.env")
    test_path.touch()
    manager_mock._storage.exists.return_value = True
    manager_mock._storage.load.return_value = "/tmp/pycarol_test/test_sa.env"
    manager_mock._tmp_filepath = Path("/tmp/pycarol_test/test_sa2.env")
    token = pycarol.bigquery.TokenManager._load_token_cloud(manager_mock)
    assert token == manager_mock._load_token_file.return_value


@mock.patch("pycarol.bigquery.Token")
def test_token_manager_get_forced_token(token_mock) -> None:
    manager_mock = mock.MagicMock()
    token = pycarol.bigquery.TokenManager.get_forced_token(manager_mock)
    assert token == token_mock.return_value


@mock.patch("pycarol.bigquery.Token")
def test_token_manager_get_token(token_mock) -> None:
    """Test the get_token() method of the pycarol.bigquery.TokenManager class."""
    manager_mock = mock.MagicMock()
    token = pycarol.bigquery.TokenManager.get_token(manager_mock)
    assert token == manager_mock.get_forced_token.return_value


@mock.patch("pycarol.bigquery.TokenManager")
def test_bq_init(manager_mock) -> None:
    """Test the initialization of the BQ class in the pycarol.bigquery module."""
    bq_mock = mock.MagicMock()
    carol_mock = mock.MagicMock()
    carol_mock.app_name = " "
    carol_mock.get_current.return_value = {
        "env_name": "env_name",
        "env_id": "env_id",
        "org_name": "org_name",
        "org_id": "org_id",
        "org_level": False,
    }
    pycarol.bigquery.BQ.__init__(bq_mock, carol_mock)
    assert bq_mock._env == {
        "env_name": "env_name",
        "env_id": "env_id",
        "org_name": "org_name",
        "org_id": "org_id",
        "org_level": False,
        "app_name": " ",
    }
    assert bq_mock._project_id == "carol-env_id"
    assert bq_mock._dataset_id == "carol-env_id.env_id"
    assert bq_mock._token_manager == manager_mock.return_value
    assert pycarol.bigquery.BQ._build_query_job_labels(bq_mock) == {
        "tenant_id": "env_id",
        "tenant_name": "env_name",
        "organization_id": "org_id",
        "organization_name": "org_name",
        "job_type": "sync",
        "source": "py_carol",
    }


@mock.patch("pycarol.bigquery.Credentials")
@mock.patch("pycarol.bigquery.bigquery")
def test_bq_generate_client(bigquery_mock, credentials_mock) -> None:
    """Test the _generate_client() method of the pycarol.bigquery.BQ class."""
    sa = {"project_id": ""}
    client = pycarol.bigquery.BQ._generate_client(sa)
    assert client == bigquery_mock.Client.return_value


@mock.patch("pycarol.bigquery.bigquery.QueryJobConfig")
def test_bq_query_pd(query_job_mock) -> None:
    """Test the query() method of the pycarol.bigquery.BQ class."""
    import pandas as pd

    bq_mock = mock.MagicMock()
    query_ret = [
        {"col1": "val1", "col2": "val2"},
        {"col1": "val1", "col2": "val2"},
    ]
    client_mock = mock.MagicMock()
    client_mock.query.return_value = query_ret
    bq_mock._generate_client.return_value = client_mock
    query = ""
    ret = pycarol.bigquery.BQ.query(bq_mock, query, return_dataframe=True)
    assert ret.equals(pd.DataFrame(query_ret))  # type: ignore


@mock.patch("pycarol.bigquery.bigquery.QueryJobConfig")
def test_bq_query(query_job_mock) -> None:
    """Test the query() method of the pycarol.bigquery.BQ class."""
    bq_mock = mock.MagicMock()
    query_ret = [
        {"col1": "val1", "col2": "val2"},
        {"col1": "val1", "col2": "val2"},
    ]
    client_mock = mock.MagicMock()
    client_mock.query.return_value = query_ret
    bq_mock._generate_client.return_value = client_mock
    query = ""
    ret = pycarol.bigquery.BQ.query(bq_mock, query, return_dataframe=False)
    assert ret == query_ret


@mock.patch("pycarol.bigquery.TokenManager")
def test_storage_init(manager_mock) -> None:
    """Test the initialization of the BQStorage class in the pycarol.bigquery module."""
    storage_mock = mock.MagicMock()
    carol_mock = mock.MagicMock()
    carol_mock.get_current.return_value = {"env_id": "5"}
    pycarol.bigquery.BQStorage.__init__(storage_mock, carol_mock)
    assert storage_mock._env == {"env_id": "5"}
    assert storage_mock._project_id == "carol-5"
    assert storage_mock._dataset_id == "5"
    assert storage_mock._token_manager == manager_mock.return_value


@mock.patch("pycarol.bigquery.Credentials")
@mock.patch("pycarol.bigquery.bigquery_storage")
def test_storage_generate_client(bigquery_mock, credentials_mock) -> None:
    """Test the _generate_client() method of the pycarol.bigquery.BQStorage class."""
    sa = {"project_id": ""}
    client = pycarol.bigquery.BQStorage._generate_client(sa)
    assert client == bigquery_mock.BigQueryReadClient.return_value


@mock.patch("pycarol.bigquery.types")
def test_storage_get_read_session(types_mock) -> None:
    """Test the _get_read_session() method of the pycarol.bigquery.BQStorage class."""
    storage_mock = mock.MagicMock()
    client_mock = mock.MagicMock()
    session = pycarol.bigquery.BQStorage._get_read_session(
        storage_mock, client_mock, "table"
    )
    assert session == client_mock.create_read_session.return_value


def test_storage_query_pd() -> None:
    """Test the query() method of the pycarol.bigquery.BQStorage class."""
    import pandas as pd

    pages = mock.MagicMock()
    page1 = mock.MagicMock()
    page1.to_dataframe.return_value = pd.DataFrame(
        [
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
        ]
    )
    page2 = mock.MagicMock()
    page2.to_dataframe.return_value = pd.DataFrame(
        [
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
        ]
    )
    pages.pages = [page1, page2]
    reader_mock = mock.MagicMock()
    reader_mock.rows.return_value = pages

    client_mock = mock.MagicMock()
    client_mock.read_rows.return_value = reader_mock

    storage_mock = mock.MagicMock()
    storage_mock._generate_client.return_value = client_mock
    ret = pycarol.bigquery.BQStorage.query(storage_mock, "table")

    ret_expected = pd.DataFrame(
        [
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
        ]
    )
    assert ret_expected.equals(ret)


def test_storage_query() -> None:
    """Test the query() method of the pycarol.bigquery.BQStorage class."""
    pages = mock.MagicMock()
    page1 = [
        {"col1": "name1", "col2": "name2"},
        {"col1": "name1", "col2": "name2"},
    ]
    page2 = [
        {"col1": "name1", "col2": "name2"},
        {"col1": "name1", "col2": "name2"},
    ]
    pages.pages = [page1, page2]
    reader_mock = mock.MagicMock()
    reader_mock.rows.return_value = pages

    client_mock = mock.MagicMock()
    client_mock.read_rows.return_value = reader_mock

    storage_mock = mock.MagicMock()
    storage_mock._generate_client.return_value = client_mock
    ret = pycarol.bigquery.BQStorage.query(
        storage_mock, "table", return_dataframe=False
    )

    ret_expected = [
        [
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
        ],
        [
            {"col1": "name1", "col2": "name2"},
            {"col1": "name1", "col2": "name2"},
        ],
    ]
    assert ret_expected == ret
