import pandas as pd
from dask import dataframe as dd
from .. import __BUCKET_NAME__


def _build_url_parquet(tenant_id, dm_name):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{dm_name}/golden/*.parquet'

def _import_dask(url, access_id, access_key, aws_session_token, merge_records=False):
    d = dd.read_parquet(url, storage_options={"key": access_id,
                                              "secret": access_key,
                                              "token":aws_session_token})

    return d


def _import_pandas():
    pass