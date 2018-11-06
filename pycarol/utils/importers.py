import pandas as pd
from dask import dataframe as dd
from .. import __BUCKET_NAME__
import io
from joblib import Parallel, delayed


def _get_file_paths_golden(s3, tenant_id, dm_name):
    bucket = s3.Bucket(__BUCKET_NAME__)
    parq =list(bucket.objects.filter(Prefix=f'carol_export/{tenant_id}/{dm_name}/golden'))
    return [i.key for i in parq if i.key.endswith('.parquet')]


def _get_file_paths_staging(s3, tenant_id, connector_id,staging_name):
    bucket = s3.Bucket(__BUCKET_NAME__)
    parq =list(bucket.objects.filter(Prefix=f'carol_export/{tenant_id}/{connector_id}_{staging_name}/staging'))
    return [i.key for i in parq if i.key.endswith('.parquet')]

def _build_url_parquet_golden(tenant_id, dm_name):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{dm_name}/golden/'

def _build_url_parquet_staging(tenant_id, staging_name, connector_id):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{connector_id}_{staging_name}/staging/'


def _import_dask(tenant_id, access_id, access_key, aws_session_token, merge_records=False,
                 dm_name=None,golden=False,
                 connector_id=None, staging_name=None):

    #TODO: merge_records
    if golden:
        url = _build_url_parquet_golden(tenant_id=tenant_id,
                                        dm_name=dm_name)
    else:
        url = _build_url_parquet_staging(tenant_id=tenant_id,
                                         staging_name=staging_name, connector_id=connector_id)

    url = url + '*.parquet'
    d = dd.read_parquet(url, storage_options={"key": access_id,
                                              "secret": access_key,
                                              "token":aws_session_token})

    return d.compute()


def _import_pandas(s3, tenant_id, dm_name=None,connector_id=None,
                   staging_name=None, n_jobs=1, verbose=10, golden=False):

    if golden:
        file_paths = _get_file_paths_golden(s3=s3, tenant_id=tenant_id, dm_name=dm_name)
    else:
        file_paths = _get_file_paths_staging(s3=s3, tenant_id=tenant_id, staging_name=staging_name,
                                             connector_id=connector_id)
    if n_jobs==1:
        df_list = []
        for i,file in enumerate(file_paths):
            print(i)
            obj=s3.Object(__BUCKET_NAME__, file)
            buffer = io.BytesIO()
            obj.download_fileobj(buffer)
            df_list.append(pd.read_parquet(buffer))
        if not df_list:
            return []
        return pd.concat(df_list, ignore_index=True)

    else:
        raise Exception('need to think how to pickle the objects')
        list_to_compute = Parallel(n_jobs=n_jobs,
                                   verbose=verbose)(delayed(_par_paquet)(
                                                            s3,file
                                                        )
                                                    for file in file_paths)

        df = pd.concat(list_to_compute, ignore_index=True)
        return df


def _par_paquet(s3,file):
    s3.Object(__BUCKET_NAME__, file)
    buffer = io.BytesIO()
    return object.download_fileobj(buffer)