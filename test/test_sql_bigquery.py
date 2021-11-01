import json
import typing as T

from dotenv import load_dotenv
import pandas as pd

import pycarol

SA_FILEPATH = "/home/jro/wk/totvs/sa.json"
TEST_QUERY1 = """
    SELECT *
    FROM `4c2c9090e7c611e893bf0e900682978b.dm_clockinrecords`
    LIMIT 100
"""


def _setup_service_account() -> T.Dict[str, str]:
    with open(SA_FILEPATH, "r") as file:
        service_account = json.loads(file.read())
    return service_account


def _setup_login() -> pycarol.Carol:
    load_dotenv()
    return pycarol.Carol()


def test_query():
    carol = _setup_login()
    service_account = _setup_service_account()
    result = pycarol.bigquery.query(carol, TEST_QUERY1, service_account).to_dataframe()
    assert isinstance(result, pd.DataFrame), "result must be a Pandas DataFrame."
    assert not result.empty, "result must not be empty."


def test_staging_pure_query():
    carol = _setup_login()
    service_account = _setup_service_account()
    sql = pycarol.SQL(carol)
    result = sql.query(TEST_QUERY1, method="bigquery", service_account=service_account)
    assert isinstance(result, pd.DataFrame), "result must be a Pandas DataFrame."
    assert not result.empty, "result must not be empty."


def test_staging_pure_query_dict():
    carol = _setup_login()
    service_account = _setup_service_account()
    sql = pycarol.SQL(carol)
    result = sql.query(
        TEST_QUERY1, method="bigquery", service_account=service_account, dataframe=False
    )
    assert isinstance(result, T.List), "result must be a list."
    assert len(result) > 1, "result must not be empty."


def test_staging_templated_query():
    test_query = """
        SELECT *
        FROM {{clockinmobile.clockinrecords}}
        LIMIT 100
    """
    carol = _setup_login()
    service_account = _setup_service_account()
    sql = pycarol.SQL(carol)
    result = sql.query(test_query, method="bigquery", service_account=service_account)
    assert isinstance(result, pd.DataFrame), "result must be a Pandas DataFrame."
    assert not result.empty, "result must not be empty."


def test_model_templated_query():
    test_query = """
        SELECT *
        FROM {{clockinrecords}}
        LIMIT 100
    """
    carol = _setup_login()
    service_account = _setup_service_account()
    sql = pycarol.SQL(carol)
    result = sql.query(test_query, method="bigquery", service_account=service_account)
    assert isinstance(result, pd.DataFrame), "result must be a Pandas DataFrame."
    assert not result.empty, "result must not be empty."


def test_mix_templated_query():
    test_query = """
        SELECT mdmId
        FROM {{clockinmobile.clockinrecords}}
        UNION ALL
        SELECT mdmId
        FROM {{clockinrecords}}
        LIMIT 100
    """
    carol = _setup_login()
    service_account = _setup_service_account()
    sql = pycarol.SQL(carol)
    result = sql.query(test_query, method="bigquery", service_account=service_account)
    assert isinstance(result, pd.DataFrame), "result must be a Pandas DataFrame."
    assert not result.empty, "result must not be empty."


if __name__ == "__main__":
    test_query()
    test_staging_pure_query()
    test_staging_pure_query_dict()
    test_staging_templated_query()
    test_model_templated_query()
    test_mix_templated_query()
