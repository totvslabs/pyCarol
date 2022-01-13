import json
import typing as T

from dotenv import load_dotenv
import pandas as pd

import pycarol

SA_FILEPATH = "/mnt/c/Users/juvenal.jose/Documents/bigquery.json"

TEST_QUERY1 = """
    SELECT *
    FROM `4c2c9090e7c611e893bf0e900682978b.dm_clockinrecords`
    LIMIT 1000
"""


def _setup_service_account() -> T.Dict[str, str]:
    with open(SA_FILEPATH, "r") as file:
        service_account = json.loads(file.read())
    return service_account


def _setup_login() -> pycarol.Carol:
    load_dotenv()
    return pycarol.Carol()


def test_paginated_query():
    login = _setup_login()
    service_account = _setup_service_account()

    ps = 100
    bq = pycarol.bigquery.BQ(login, service_account=service_account)	
    df_first_page, c = bq.paginated_query(TEST_QUERY1, page_size=ps, return_dataframe=True)

    assert isinstance(df_first_page, pd.DataFrame), "result must be a Pandas DataFrame."
    assert not df_first_page.empty, "result must not be empty."
    assert df_first_page.shape[0] == ps, "first page of results must contain 100 records."

    assert isinstance(c, dict), "control variable must be a dict."
    assert c["job_id"] is not None, f"job_id from control dict must be an int, received " + c["job_id"] + "."
    assert isinstance(c["total_pages"], int), f"total_pages from control dict must be an int, received " + c["total_pages"] + "."
    assert c["total_records"] > 0, f"total_records from control dict must be greater than zero, received " + c["total_records"] + "."
    assert c["current_page"] == 0, f"current_page from control dict must be zero at the first call, received " + c["current_page"] + "."


def test_fetch_page():
    login = _setup_login()
    service_account = _setup_service_account()

    bq = pycarol.bigquery.BQ(login, service_account=service_account)

    ps = 200
    df_first_page, c1 = bq.paginated_query(TEST_QUERY1, page_size=ps, return_dataframe=True)
    df_second_page, c2 = bq.fetch_page(job_id=c1["job_id"], page_size=ps, page=1, return_dataframe=True)
    df_fourth_page, c4 = bq.fetch_page(job_id=c2["job_id"], page_size=ps, page=3, return_dataframe=True)
    df_back_to_sec, c21 = bq.fetch_page(job_id=c2["job_id"], page_size=ps, page=1, return_dataframe=True)

    assert not df_first_page.empty, "first page must not be empty."
    assert df_first_page.shape[0] == int(ps), f"first page must contain {ps} records, found " + str(df_first_page.shape[0]) + "."
    assert not df_second_page.empty, "second page must not be empty."
    assert df_second_page.shape[0] == int(ps), f"second page must contain {ps} records, found " + str(df_second_page.shape[0]) + "."

    assert isinstance(c2, dict), "control variable must be a dict."
    assert c2["job_id"] == c1["job_id"], f"job_id from the first and subsequent queries must be equal, found " + c1["job_id"] + " and " + c2["job_id"] + "."

    assert isinstance(c2["total_pages"], (int)), "total_pages from control dict must be an int."
    assert c2["total_records"] > 0, "total_records from control dict must be greater than zero."
    assert c2["current_page"] == 1, f"current_page from control dict must be 1."
    assert c4["current_page"] == 3, f"current_page from control dict must be 3."

    assert df_back_to_sec.equals(df_second_page), "going back and forth through the iterator should bring the same results for the same page"


if __name__ == "__main__":
    test_paginated_query()
    test_fetch_page()
