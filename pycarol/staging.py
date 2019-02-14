import pandas as pd
import json
import itertools
import warnings
import asyncio

from .query import Query, delete_golden
from .schema_generator import carolSchemaGenerator
from .connectors import Connectors
from .carolina import Carolina
from .utils.importers import _import_dask, _import_pandas
from .filter import Filter, TYPE_FILTER
from .utils import async_helpers
from .utils.miscellaneous import stream_data


_SCHEMA_TYPES_MAPPING = {
    "geo_point": str,
    "long": int,
    "double": float,
    "nested": str,
    "string": str,
    "base64": str,
    "boolean": bool
}


class Staging:
    def __init__(self, carol):
        self.carol = carol

    def send_data(self, staging_name, data=None, connector_name=None, connector_id=None, step_size=100,
                  print_stats=True,
                  gzip=True, auto_create_schema=False, crosswalk_auto_create=None, force=False, max_workers=None,
                  dm_to_delete=None, async_send=False):
        '''
        :param staging_name:  `str`,
            Staging name to send the data.
        :param data: pandas data frame, json. default `None`
            Data to be send to Carol
        :param connector_name: `str`, default `None`
            Connector name where the staging should be.
        :param connector_id: `str`, default `None`
            Connector Id where the staging should be.
        :param step_size: `int`, default `100`
            Number of records to be sent in each iteration. Max size for each batch is 10MB
        :param print_stats: `bool`, default `True`
            If print the status
        :param gzip: `bool`, default `True`
            If send each batch as a gzip file.
        :param auto_create_schema: `bool`, default `False`
            If to auto create the schema for the data being sent.
        :param crosswalk_auto_create: `list`, default `None`
            If `auto_create_schema=True`, one should send the crosswalk for the staging.
        :param force: `bool`, default `False`
            If `force=True` it will not check for repeated records according to crosswalk. If `False` it will check for
            duplicates and raise an error if so.
        :param max_workers: `int`, default `None`
            To be used with `async_send=True`. Number of threads to use when sending.
        :param dm_to_delete: `str`, default `None`
            Name of the data model to be erased before send the data.
        :param async_send: `bool`, default `False`
            To use async to send the data. This is much faster than a sequential send.
        :return: None
        '''

        self.gzip = gzip
        extra_headers = {}
        content_type = 'application/json'
        if self.gzip:
            content_type = None
            extra_headers["Content-Encoding"] = "gzip"
            extra_headers['content-type'] = 'application/json'

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            if connector_id is None:
                connector_id = self.carol.connector_id

        schema = self.get_schema(staging_name, connector_id=connector_id)

        is_df = False
        if isinstance(data, pd.DataFrame):
            is_df = True
            data_size = data.shape[0]
            _sample_json = data.iloc[0].to_json(date_format='iso')
        elif isinstance(data, str):
            data = json.loads(data)
            data_size = len(data)
            _sample_json = data[0]
        else:
            data_size = len(data)
            _sample_json = data[0]

        if (not isinstance(data, list)) and (not is_df):
            data = [data]
            data_size = len(data)

        if (not schema) and (auto_create_schema):
            assert crosswalk_auto_create, "You should provide a crosswalk"
            self.create_schema(_sample_json, staging_name, connector_id=connector_id,
                               crosswalk_list=crosswalk_auto_create)
            _crosswalk = crosswalk_auto_create
            print('provided crosswalk ', _crosswalk)
        elif auto_create_schema:
            assert crosswalk_auto_create, "You should provide a crosswalk"
            self.create_schema(_sample_json, staging_name, connector_id=connector_id,
                               crosswalk_list=crosswalk_auto_create, overwrite=True)
            _crosswalk = crosswalk_auto_create
            print('provided crosswalk ', _crosswalk)
        else:
            _crosswalk = schema["mdmCrosswalkTemplate"]["mdmCrossreference"].values()
            _crosswalk = list(_crosswalk)[0]
            print('fetched crosswalk ', _crosswalk)

        if is_df and not force:
            assert data.duplicated(subset=_crosswalk).sum() == 0, \
                "crosswalk is not unique on dataframe. set force=True to send it anyway."

        if dm_to_delete is not None:
            delete_golden(self.carol, dm_to_delete)

        url = f'v2/staging/tables/{staging_name}?returnData=false&connectorId={connector_id}'
        self.cont = 0
        if async_send:
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(async_helpers.send_data_asynchronous(carol=self.carol,
                                                                                data=data, data_size=data_size,
                                                                                step_size=step_size, is_df=is_df,
                                                                                url=url,
                                                                                extra_headers=extra_headers,
                                                                                content_type=content_type,
                                                                                max_workers=max_workers,
                                                                                compress_gzip=self.gzip))
            loop.run_until_complete(future)


        else:
            for data_json, cont in stream_data(data=data, data_size=data_size,
                                               step_size=step_size, is_df=is_df,
                                               compress_gzip=self.gzip):

                self.carol.call_api(url, data=data_json, extra_headers=extra_headers, content_type=content_type)
                if print_stats:
                    print('{}/{} sent'.format(cont, data_size), end='\r')


    def get_schema(self, staging_name, connector_name=None, connector_id=None):

        query_string = None
        if connector_name:
            connector_id = self._connector_by_name(connector_name)

        if connector_id:
            query_string = {"connectorId": connector_id}
        try:
            return self.carol.call_api(f'v2/staging/tables/{staging_name}/schema', method='GET',
                                       params=query_string)
        except Exception:
            return None

    def create_schema(self, fields_dict, staging_name, connector_id=None, mdm_flexible='false',
                      crosswalk_name=None, crosswalk_list=None, overwrite=False):
        assert fields_dict is not None

        if isinstance(fields_dict, list):
            fields_dict = fields_dict[0]

        if isinstance(fields_dict, dict):
            schema = carolSchemaGenerator(fields_dict)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name, crosswalkList=crosswalk_list)
        elif isinstance(fields_dict, str):
            schema = carolSchemaGenerator.from_json(fields_dict)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name, crosswalkList=crosswalk_list)
        else:
            print('Behavior for type %s not defined!' % type(fields_dict))

        self.send_schema(schema=schema, staging_name=staging_name, connector_id=connector_id, overwrite=overwrite)

    def send_schema(self, schema, staging_name, connector_id=None, overwrite=False):
        query_string = {"connectorId": connector_id}
        if connector_id is None:
            connector_id = self.carol.connector_id
            query_string = {"connectorId": connector_id}

        has_schema = self.get_schema(staging_name, connector_id=connector_id) is not None
        if has_schema and overwrite:
            method = 'PUT'
        else:
            method = 'POST'

        resp = self.carol.call_api('v2/staging/tables/{}/schema'.format(staging_name), data=schema, method=method,
                                   params=query_string)

    def _check_crosswalk_in_data(self, schema, _sample_json):
        crosswalk = schema["mdmCrosswalkTemplate"]["mdmCrossreference"].values()
        if all(name in _sample_json for name in crosswalk):
            pass

    def _connector_by_name(self, connector_name):
        """
        Get connector id given connector name

        :param connector_name: `str`
            Connector name
        :return: `str`
            Connector Id
        """
        return Connectors(self.carol).get_by_name(connector_name)['mdmId']

    def fetch_parquet(self, staging_name, connector_id=None, connector_name=None, backend='dask', verbose=0,
                      merge_records=True, n_jobs=1, return_dask_graph=False, columns=None, max_hits=None,
                      return_metadata=False, callback=None):
        """

        Fetch parquet from a staging table.

        :param staging_name: `str`,
            Staging name to fetch parquet of
        :param connector_id: `str`, default `None`
            Connector id to fetch parquet of
        :param connector_name: `str`, default `None`
            Connector name to fetch parquet of
        :param verbose: `int`, default `0`
            Verbosity
        :param backend: ['dask','pandas'], default `dask`
            if to use either dask or pandas to fetch the data
        :param merge_records: `bool`, default `True`
            This will keep only the most recent record exported. Sometimes there are updates and/or deletions and
            one should keep only the last records.
        :param n_jobs: `int`, default `1`
            To be used with `backend='pandas'`. It is the number of threads to load the data from carol export.
        :param return_dask_graph: `bool`, default `false`
            If to return the dask graph or the dataframe.
        :param columns: `list`, default `None`
            List of columns to fetch.
        :param max_hits: `int`, default `None`
            Number of records to get. This only should be user for tests.
        :param return_metadata: `bool`, default `False`
            To return or not the fields ['mdmId', 'mdmCounterForEntity']
        :return:
        """

        old_columns = None
        if columns:
            old_columns = columns
            columns = [i.replace("-", "_") for i in columns]
            columns.extend(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'])
            old_columns = dict(zip([i.replace("-", "_") for i in columns], old_columns))

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        assert backend == 'dask' or backend == 'pandas'

        if return_dask_graph:
            assert backend == 'dask'

        # validate export
        stags = self._get_staging_export_stats()
        if (not stags.get(staging_name)):
            raise Exception(f'"{staging_name}" is not set to export data, \n '
                            f'use `dm = Staging(login).export(staging_name="{staging_name}",'
                            f'connector_id="{connector_id}", sync_staging=True) to activate')

        if stags.get(staging_name)['mdmConnectorId'] != connector_id:
            raise Exception(
                f'"Wrong connector Id {connector_id}. The connector Id associeted to this staging is  '
                f'{stags.get(staging_name)["mdmConnectorId"]}"')

        carolina = Carolina(self.carol)
        carolina._init_if_needed()
        if backend == 'dask':
            access_id = carolina.ai_access_key_id
            access_key = carolina.ai_secret_key
            aws_session_token = carolina.ai_access_token

            d = _import_dask(tenant_id=self.carol.tenant['mdmId'], connector_id=connector_id, staging_name=staging_name,
                             access_key=access_key, access_id=access_id, aws_session_token=aws_session_token,
                             merge_records=merge_records, golden=False, return_dask_graph=return_dask_graph,
                             columns=columns, max_hits=max_hits)

        elif backend == 'pandas':
            s3 = carolina.s3
            d = _import_pandas(s3=s3, tenant_id=self.carol.tenant['mdmId'], connector_id=connector_id, verbose=verbose,
                               staging_name=staging_name, n_jobs=n_jobs, golden=False, columns=columns,
                               max_hits=max_hits, callback=callback)

            # TODO: Do the same for dask backend
            if d is None:
                warnings.warn(f'No data to fetch! {staging_name} has no data', UserWarning)
                cols_keys = list(self.get_schema(
                    staging_name=staging_name, connector_name=connector_name
                )['mdmStagingMapping']['properties'].keys())
                if return_metadata:
                    cols_keys.extend(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'])

                elif columns:
                    columns = [i for i in columns if i not in ['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated']]

                d = pd.DataFrame(columns=cols_keys)
                for key, value in self.get_schema(staging_name=staging_name,
                                                  connector_name=connector_name)['mdmStagingMapping'][
                    'properties'].items():
                    d.loc[:, key] = d.loc[:, key].astype(_SCHEMA_TYPES_MAPPING.get(value['type'], str), copy=False)
                if columns:
                    d = d[list(set(columns))]
                return d
        else:
            raise ValueError(f'backend should be "dask" or "pandas" you entered {backend}')

        if merge_records:
            if not return_dask_graph:
                if old_columns is not None:
                    d.rename(columns=old_columns, inplace=True)
                d.sort_values('mdmCounterForEntity', inplace=True)
                d.reset_index(inplace=True, drop=True)
                d.drop_duplicates(subset='mdmId', keep='last', inplace=True)
                if not return_metadata:
                    d.drop(columns=['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'], inplace=True)
                d.reset_index(inplace=True, drop=True)
            else:
                if old_columns is not None:
                    d.rename(columns=old_columns, inplace=True)
                d = d.set_index('mdmCounterForEntity', sorted=True) \
                    .drop_duplicates(subset='mdmId', keep='last') \
                    .reset_index(drop=True)
                if not return_metadata:
                    d = d.drop(columns=['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'])

        return d

    def export(self, staging_name, connector_id=None, connector_name=None, sync_staging=True, full_export=False,
               delete_previous=False):
        """

        Export Staging to s3

        This method will trigger or pause the export of the data in the staging to
        s3.

        :param staging_name: `str`, default `None`
            Datamodel Name
        :param sync_staging: `bool`, default `True`
            Sync the data model
        :param connector_name: `str`
            Connector name
        :param connector_id: `str`
            Connector id
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None


        Usage:
        To trigger the export the first time:

        >>>from pycarol.staging import Staging
        >>>from pycarol.auth.PwdAuth import PwdAuth
        >>>from pycarol.carol import Carol
        >>>login = Carol()
        >>>stag  = Staging(login)
        >>>stag.export(staging, connector_name=connector_name,sync_staging=True)

        To do a resync, that is, start the sync from the begining without delete old data
        >>>stag.export(staging, connector_name=connector_name,sync_staging=True, full_export=True)

        To delete the old data:
        >>>stag.export(staging, connector_name=connector_name,sync_staging=True, full_export=True, delete_previous=True)

        To Pause a sync:
        >>>stag.export(staging, connector_name=connector_name,sync_staging=False)
        """

        if sync_staging:
            status = 'RUNNING'
        else:
            status = 'PAUSED'

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        query_params = {"status": status, "fullExport": full_export,
                        "deletePrevious": delete_previous}
        url = f'v2/staging/{connector_id}/{staging_name}/exporter'
        return self.carol.call_api(url, method='POST', params=query_params)

    def export_all(self, connector_id=None, connector_name=None, sync_staging=True, full_export=False,
                   delete_previous=False):
        """

        Export all Stagings from a connector to s3

        This method will trigger or pause the export of all stagings  to
        s3.

        :param sync_staging: `bool`, default `True`
            Sync the data model
        :param connector_name: `str`
            Connector name
        :param connector_id: `str`
            Connector id
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None

        Usage: See `Staging.export()`
        """
        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        conn_stats = Connectors(self.carol).stats(connector_id=connector_id)

        for staging in conn_stats.get(connector_id):
            self.export(staging_name=staging, connector_id=connector_id,
                        sync_staging=sync_staging, full_export=full_export, delete_previous=delete_previous)

    def _get_staging_export_stats(self):
        """
        Get export status for data models

        :return: `dict`
            dict with the information of which staging table is exporting its data.
        """

        query = Query(self.carol, index_type='CONFIG', only_hits=False)

        json_q = Filter.Builder(key_prefix="") \
            .must(TYPE_FILTER(value="mdmStagingDataExport")).build().to_json()

        query.query(json_q, ).go()
        staging_results = query.results
        staging_results = [elem.get('hits', elem) for elem in staging_results
                           if elem.get('hits', None)]
        staging_results = list(itertools.chain(*staging_results))
        if staging_results is not None:
            return {i['mdmStagingType']: i for i in staging_results}
