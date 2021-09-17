import json
import typing as T

from dotenv import load_dotenv
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
    service_account = _setup_service_account()
    client = pycarol.bigquery.generate_client(service_account)
    result = pycarol.bigquery.query(client, TEST_QUERY1)
    print(result)


def test_staging_pure_query():
    carol = _setup_login()
    service_account = _setup_service_account()
    result = pycarol.Staging(carol).query(TEST_QUERY1, service_account=service_account)
    print(result)


def test_staging_templated_query():
    test_query = """
        SELECT *
        FROM {{clockinmobile.clockinrecords}}
        LIMIT 100
    """
    carol = _setup_login()
    service_account = _setup_service_account()
    result = pycarol.Staging(carol).query(test_query, service_account=service_account)
    print(result)


def test_model_templated_query():
    test_query = """
        SELECT *
        FROM {{clockinrecords}}
        LIMIT 100
    """
    carol = _setup_login()
    service_account = _setup_service_account()
    result = pycarol.Staging(carol).query(test_query, service_account=service_account)
    print(result)


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
    result = pycarol.Staging(carol).query(test_query, service_account=service_account)
    print(result)


if __name__ == "__main__":
    # test_query()
    # test_staging_pure_query()
    # test_staging_templated_query()
    # test_model_templated_query()
    test_mix_templated_query()
