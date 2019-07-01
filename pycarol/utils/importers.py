import pandas as pd
from dask import dataframe as dd

from tqdm import tqdm

__STAGING_FIELDS = ['mdmCounterForEntity', 'mdmId']
__DM_FIELDS = ['mdmCounterForEntity', 'mdmId']


def _import_dask(storage, merge_records=False,
                 dm_name=None, import_type='staging', return_dask_graph=False,
                 connector_id=None, staging_name=None, view_name=None, columns=None,
                 max_hits=None, mapping_columns=None):
    if columns:
        columns = list(set(columns))
        columns += __STAGING_FIELDS
        columns = list(set(columns))

    if import_type=='golden':
        url = [storage.build_url_parquet_golden(dm_name=dm_name)]
    elif import_type == 'staging':
        url = []
        url1 = storage.build_url_parquet_staging(staging_name=staging_name, connector_id=connector_id)
        if url1 is not None:
            url.append(url1)

        url2 = storage.build_url_parquet_staging_master(staging_name=staging_name, connector_id=connector_id)
        if url2 is not None:
            url.append(url2)

        url3 = storage.build_url_parquet_staging_rejected(staging_name=staging_name, connector_id=connector_id)
        if url3 is not None:
            url.append(url3)
    elif import_type == 'view':
        url = [storage.build_url_parquet_view(view_name=view_name)]
    else:
        raise KeyError('import_type should be `golden`,`staging` or `view`')



    d = dd.read_parquet(url, storage_options=storage.get_dask_options(), columns=columns)

    d= d.rename(columns=mapping_columns)
    if return_dask_graph:
        return d
    else:
        return d.compute()


def _import_pandas(storage, dm_name=None, connector_id=None, columns=None, mapping_columns=None,
                   staging_name=None, view_name=None, import_type='staging', golden=False, max_hits=None, callback=None):
    if columns:
        columns = list(set(columns))
        columns += __DM_FIELDS
        columns = list(set(columns))

    if import_type=='golden':
        file_paths = storage.get_golden_file_paths(dm_name=dm_name)
    elif import_type=='staging':
        file_paths = storage.get_staging_file_paths(staging_name=staging_name, connector_id=connector_id)
    elif import_type == 'view':
        file_paths = storage.get_view_file_paths(view_name=view_name)
    else:
        raise KeyError('import_type should be `golden`,`staging` or `view`')

    df_list = []
    count = 0
    for i, file in enumerate(tqdm(file_paths)):
        buffer = storage.load(file['name'], format='raw', cache=False, storage_space=file['storage_space'])
        result = pd.read_parquet(buffer, columns=columns)

        if mapping_columns is not None:
            result.rename(columns=mapping_columns, inplace=True) #fix columns names (we replace `-` for `_` due to parquet limitations.
        if callback:
            assert callable(callback), \
                f'"{callback}" is a {type(callback)} and is not callable. This variable must be a function/class.'
            result = callback(result)

        df_list.append(result)
        if max_hits is not None:
            count_old = count
            count += len(df_list[i])
            if count >= max_hits:
                df_list[i] = df_list[i].iloc[:max_hits - count_old]
                break
    if not df_list:
        return None
    return pd.concat(df_list, ignore_index=True)
