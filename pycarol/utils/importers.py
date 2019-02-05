import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from tqdm import tqdm
from dask import dataframe as dd
from .. import __BUCKET_NAME__
from joblib import Parallel, delayed

__STAGING_FIELDS = ['mdmCounterForEntity','mdmId']
__DM_FIELDS = ['mdmCounterForEntity','mdmId']


def _get_file_paths_golden(s3, tenant_id, dm_name):

    """

    Get all files to be imported from s3

    :param s3: `Boto3 Object`
        OBject to handle s3 calls
    :param tenant_id: `str`
        Tenant ID
    :param dm_name: `str`
        Data model name
    :return: `list`
        List of paths of files to be imported.
    """
    bucket = s3.Bucket(__BUCKET_NAME__)
    parq =list(bucket.objects.filter(Prefix=f'carol_export/{tenant_id}/{dm_name}/golden'))
    return [i.key for i in parq if i.key.endswith('.parquet')]


def _get_file_paths_staging(s3, tenant_id, connector_id,staging_name):
    """

    :param s3: `Boto3 Object`
        OBject to handle s3 calls
    :param tenant_id:
    :param connector_id:
    :param staging_name: `str`,
            Staging name
    :return: `list`
        List of paths of files to be imported.
    """
    bucket = s3.Bucket(__BUCKET_NAME__)
    parq_stag =list(bucket.objects.filter(Prefix=f'carol_export/{tenant_id}/{connector_id}_{staging_name}/staging'))
    parq_master = list(bucket.objects.filter(
        Prefix=f'carol_export/{tenant_id}/{connector_id}_{staging_name}/master_staging'))
    parq_stag_rejected = list(bucket.objects.filter(
        Prefix=f'carol_export/{tenant_id}/{connector_id}_{staging_name}/rejected_staging'))
    parq = parq_stag + parq_master + parq_stag_rejected
    return [i.key for i in parq if i.key.endswith('.parquet')]

def _build_url_parquet_golden(tenant_id, dm_name):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{dm_name}/golden/'

def _build_url_parquet_staging(tenant_id, staging_name, connector_id):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{connector_id}_{staging_name}/staging/'

def _build_url_parquet_staging_master(tenant_id, staging_name, connector_id):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{connector_id}_{staging_name}/master_staging/'

def _build_url_parquet_staging_master_rejected(tenant_id, staging_name, connector_id):
    return f's3://{__BUCKET_NAME__}/carol_export/{tenant_id}/{connector_id}_{staging_name}/rejected_staging/'


def _import_dask(tenant_id, access_id, access_key, aws_session_token, merge_records=False,
                 dm_name=None,golden=False,return_dask_graph=False,
                 connector_id=None, staging_name=None, columns=None, max_hits=None):

    if columns:
        columns = list(set(columns))
        columns +=__STAGING_FIELDS
        columns = list(set(columns))

    if golden:
        url = _build_url_parquet_golden(tenant_id=tenant_id,
                                        dm_name=dm_name)
    else:
        url = []
        url.append(_build_url_parquet_staging(tenant_id=tenant_id,
                                         staging_name=staging_name,
                                         connector_id=connector_id))
        url.append(_build_url_parquet_staging_master(tenant_id=tenant_id,
                                              staging_name=staging_name,
                                              connector_id=connector_id))
        url.append(_build_url_parquet_staging_master_rejected(tenant_id=tenant_id,
                                                     staging_name=staging_name,
                                                     connector_id=connector_id))

    url = [i + '*.parquet' for i in url]
    try:
        d = dd.read_parquet(url, storage_options={"key": access_id,
                                                  "secret": access_key,
                                                  "token":aws_session_token},
                            columns=columns)
    except:
        return None

    if return_dask_graph:
        return d
    else:
        return d.compute()


def _import_pandas(s3, tenant_id, dm_name=None,connector_id=None, columns=None,
                   staging_name=None, n_jobs=1, verbose=0, golden=False, max_hits=None):

    if columns:
        columns = list(set(columns))
        columns +=__DM_FIELDS
        columns = list(set(columns))

    if golden:
        file_paths = _get_file_paths_golden(s3=s3, tenant_id=tenant_id, dm_name=dm_name)
    else:
        file_paths = _get_file_paths_staging(s3=s3, tenant_id=tenant_id, staging_name=staging_name,
                                             connector_id=connector_id)
    if n_jobs==1:
        df_list = []
        count = 0
        for i,file in enumerate(tqdm(file_paths)):
            obj=s3.Object(__BUCKET_NAME__, file)
            buffer = io.BytesIO()
            obj.download_fileobj(buffer)
            df_list.append(pd.read_parquet(buffer,columns=columns))               
            if max_hits is not None:
                count_old = count
                count += len(df_list[i])
                if count >=max_hits:
                    df_list[i] = df_list[i].iloc[:max_hits-count_old]
                    break
        if not df_list:
            return None
        return pd.concat(df_list, ignore_index=True)

    else:
        raise NotImplementedError('need to think how to pickle the objects')
        list_to_compute = Parallel(n_jobs=n_jobs,
                                   verbose=verbose)(delayed(_par_paquet)(
                                                            s3,file
                                                        )
                                                    for file in file_paths)

        df = pd.concat(list_to_compute, ignore_index=True)
        return df

    
def _import_pyarrow(s3, tenant_id, dm_name=None,connector_id=None, columns=None,
                    staging_name=None, n_jobs=1, verbose=0, golden=False, max_hits=None):

    if columns:
        columns = list(set(columns))
        columns +=__DM_FIELDS
        columns = list(set(columns))

    if golden:
        file_paths = _get_file_paths_golden(s3=s3, tenant_id=tenant_id, dm_name=dm_name)
    else:
        file_paths = _get_file_paths_staging(s3=s3, tenant_id=tenant_id, staging_name=staging_name,
                                             connector_id=connector_id)
    if n_jobs==1:
        df_list = [None]*len(file_paths)
        count = 0
        ii = 0
        for i,file in enumerate(tqdm(file_paths)):
            obj=s3.Object(__BUCKET_NAME__, file)
            buffer = io.BytesIO()
            obj.download_fileobj(buffer)
            df_temp = pd.read_parquet(buffer,columns=columns)
            if max_hits is not None:
                count_old = count
                count += len(df_list[i])
                if count >=max_hits:
                    df_list[i] = df_list[i].iloc[:max_hits-count_old]
                    break               
            df_list[ii] = pa.RecordBatch.from_pandas(df_temp)                    
            ii = ii + 1
        if not df_list:
            return None
        return pa.Table.from_batches(df_list).to_pandas()

    else:
        raise NotImplementedError('need to think how to pickle the objects')
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