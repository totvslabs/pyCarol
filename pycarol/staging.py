import json
import warnings
import asyncio

from .schema_generator import carolSchemaGenerator
from .connectors import Connectors
from .storage import Storage
from .utils.importers import _import_dask, _import_pandas
from .utils import async_helpers
from .utils.miscellaneous import stream_data
from . import _CAROL_METADATA_STAGING, _NEEDED_FOR_MERGE, _CAROL_METADATA_UNTIE_STAGING
from .utils.miscellaneous import drop_duplicated_parquet, drop_duplicated_parquet_dask
from .utils.deprecation_msgs import _deprecation_msgs

_SCHEMA_TYPES_MAPPING = {
    "geopoint": str,
    "long": int,
    "double": float,
    "nested": str,
    "string": str,
    "base64": str,
    "date": str,
    "boolean": bool
}


class Staging:

    """

    Class to send data to Carol.

    """

    def __init__(self, carol):
        self.carol = carol

    def send_data(self, staging_name, data=None, connector_name=None, connector_id=None, step_size=500,
                  print_stats=True, gzip=True, auto_create_schema=False, crosswalk_auto_create=None,
                  flexible_schema=False, force=False,  max_workers=2,  dm_to_delete=None,
                  async_send=False, carol_data_storage=False, storage_only=True, carol_sync=False):
        """
        Send data to a staging table in Carol.

        Args:
            staging_name:  `str`,
                Staging name to send the data.
            data: pandas data frame, json. default `None`
                Data to be send to Carol
            connector_name: `str`, default `None`
                Connector name where the staging should be. Either `connector_name` or `connector_id` need to be set.
            connector_id: `str`, default `None`
                Connector Id where the staging should be. Either `connector_name` or `connector_id` need to be set.
            step_size: `int`, default `500`
                Number of records to be sent in each iteration. Max size for each batch is 10MB.
            print_stats:`bool`, default `True`
                If print the number of records sent
            gzip:`bool`, default `True`
                If send each batch as a gzip file.
            auto_create_schema:`bool`, default `False`
                If to auto create the schema for the data being sent.
            crosswalk_auto_create: `list`, default `None`
                If `auto_create_schema=True`, crosswalk list of fields.
            flexible_schema: `bool`, default `False`
                If `auto_create_schema=True`, to use a flexible schema.
            force: `bool`, default `False`
                pycarol will check for duplicated values given the crosswalk.
                If `force=True` it will not check. If `False` it will check for duplicates and raise an error.
            max_workers: `int`, default `2`
                To be used with `async_send=True`. Number of threads to use when sending.
            dm_to_delete: `str`, default `None` DEPRECATED.
                Name of the data model to be erased before send the data.
            async_send: `bool`, default `False`
                To use async to send the data. This is much faster than a sequential send.
                It can conflict with jupyter notebooks process. To run this inside a jupyter notebook, use

                .. code:: python

                    import nest_asyncio
                    nest_asyncio.apply()

            carol_data_storage: `bool`, default `False`
                Deprecated, Use `storage_only`
            storage_only: `bool`, default `False`
                Send data only to CDS.
            carol_sync: `bool`, default `False`
                Send and wait data to be processed in Carol

        """

        if dm_to_delete is not None:
            _deprecation_msgs("`dm_to_delete` is deprecated and has no action.")

        if carol_data_storage:
            _deprecation_msgs("`carol_data_storage` is deprecated and has no action.")

        if not storage_only:
            _deprecation_msgs("`storage_only` will be irrelevant. All data will be send to CDS as default.")


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

        is_df = False
        _crosswalk = None
        if isinstance(data, str):
            data = json.loads(data)
            data_size = len(data)
            _sample_json = data[0]
        elif isinstance(data, list):
            data_size = len(data)
            _sample_json = data[0]
        elif isinstance(data, dict):
            data = [data]
            data_size = len(data)
            _sample_json = data[0]
        else:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                is_df = True
                data_size = data.shape[0]
                _sample_json = data.iloc[0].to_json(date_format='iso')
            else:
                raise ValueError('`data` should be either a stringfied json, a list of dictionaries, a dictionary or a pd.DataGrame ')


        if (not isinstance(data, list)) and (not is_df):
            data = [data]
            data_size = len(data)

        if auto_create_schema:

            if crosswalk_auto_create is None:
                raise ValueError("You should provide a crosswalk. Use `crosswalk_auto_create` parameter.")

            schema = self.get_schema(staging_name, connector_id=connector_id)
            if not schema:
                overwrite = False
            else:
                overwrite = True
            self.create_schema(_sample_json, staging_name, connector_id=connector_id, export_data=carol_data_storage,
                               crosswalk_list=crosswalk_auto_create, overwrite=overwrite, mdm_flexible=flexible_schema)
            _crosswalk = crosswalk_auto_create
            print('provided crosswalk ', _crosswalk)

        if is_df and not force:
            if _crosswalk is None:
                schema = self.get_schema(staging_name, connector_id=connector_id)
                _crosswalk = schema["mdmCrosswalkTemplate"]["mdmCrossreference"].values()
                _crosswalk = list(_crosswalk)[0]
                print('fetched crosswalk ', _crosswalk)

            if data.duplicated(subset=_crosswalk).sum() >= 1:
                raise Exception("crosswalk is not unique on data frame. set force=True to send it anyway.")

        if not storage_only and not carol_sync:
            url = f'v2/staging/tables/{staging_name}&returnData=false&connectorId={connector_id}'
        elif carol_sync:
            step_size = min(100, step_size)
            url = f'v2/staging/tables/{staging_name}/sync?&connectorId={connector_id}&processMerge=true'
        else:
            url = f'v2/staging/intake/{staging_name}?returnData=false&connectorId={connector_id}'
        
        self.cont = 0
        if async_send:
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(async_helpers.send_data_asynchronous(carol=self.carol,
                                                                                data=data,
                                                                                step_size=step_size,
                                                                                url=url,
                                                                                extra_headers=extra_headers,
                                                                                content_type=content_type,
                                                                                max_workers=max_workers,
                                                                                compress_gzip=self.gzip))
            loop.run_until_complete(future)

        else:
            for data_json, cont in stream_data(data=data,
                                               step_size=step_size,
                                               compress_gzip=self.gzip):

                self.carol.call_api(url, data=data_json, extra_headers=extra_headers, content_type=content_type,
                                    status_forcelist=[502, 429, 524, 408, 504, 598, 520, 503, 500],
                                    method_whitelist=frozenset(['POST'])
                                    )
                if print_stats:
                    print('{}/{} sent'.format(cont, data_size), end='\r')

    def get_schema(self, staging_name, connector_name=None, connector_id=None):

        """

        Return the staging table schema.

        Args:
            staging_name:  `str`,
                Staging name
            connector_name: `str`, default `None`
                Connector name. Either `connector_name` or `connector_id` need to be set.
            connector_id: `str`, default `None`
                Connector Id. Either `connector_name` or `connector_id` need to be set.

        Returns: `dict`
            Staging table schema

        """

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

    def create_schema(self, fields_dict=None, staging_name=None, connector_id=None, connector_name=None,
                      mdm_flexible=False,  crosswalk_name=None, crosswalk_list=None, overwrite=False, auto_send=True,
                      export_data=False, data=None):
        """

        Args:
            fields_dict: `dict`, `list of dicts`, `pandas.DataFrame`, default `None`
                Data to create schema from. `fields_dict` will be removed in the future. Use parameter `data`
            staging_name: `str`,
                Staging name to send the data.
            connector_id: `str`, default `None`
                Connector name where the staging should be.
            connector_name: `str`, default `None`
                Connector name where the staging should be.
            mdm_flexible:  `bool`, default `False`
                If flexible schema.
            crosswalk_name: `None`, default `staging_name`
                Crosswalk name. Most of the time it should be the staging name.
            crosswalk_list: `list`, default `None`
                Crosswalk list of fields.
            overwrite: `bool`, default `False`
                if already exists, overwrite current schema
            auto_send: `bool`, default `True`
                Send the schema after creating.
            export_data: `bool`, default `False`
                Export data to CDS for this staging. This is a manual export.
            data: `json`, `list of dicts`, `pandas.DataFrame`, default `None`
                Data to create schema from.

        """
        import pandas as pd
        if export_data is not None:
            _deprecation_msgs("`export_data` is deprecated and has no action.")

        assert staging_name is not None, 'staging_name must be set.'
        assert fields_dict is not None or data is not None, 'fields_dict or df must be set'

        if fields_dict is not None:
            warnings.warn(
                "fields_dict will be deprecated, use `data`",
                DeprecationWarning, stacklevel=3
            )
            data = fields_dict

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id, f'connector_id or connector name should be set.'

        if data is not None:
            if isinstance(data, pd.DataFrame):
                data = data.iloc[0].to_dict()

        if isinstance(data, list):
            data = data[0]

        if isinstance(data, dict):
            schema = carolSchemaGenerator(data)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name, crosswalkList=crosswalk_list)
        elif isinstance(data, str):
            schema = carolSchemaGenerator.from_json(data)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name, crosswalkList=crosswalk_list)
        else:
            print('Behavior for type %s not defined!' % type(data))

        if auto_send:
            self.send_schema(schema=schema, staging_name=staging_name, connector_id=connector_id, overwrite=overwrite)
        else:
            return schema

    def send_schema(self, schema, staging_name=None, connector_id=None, connector_name=None,
                    overwrite=False):

        """

        Send schema to Carol

        Args:
            schema: `dict`
                Dictianary with the schema to be sent
            staging_name: `str`, default `None`
                Staging name to send the data. If empty it will get from the `schema`
            connector_id: `str`, default `None`
                Connector name. If empty it will get from the `schema`
            connector_name: `str`, default `None`
                Connector name. If empty it will get from the `schema`
            overwrite: `bool`, default `False`
                if already exists, overwrite current schema

        Returns: `None`

        """

        if connector_name:
            connector_id = self._connector_by_name(connector_name)

        if staging_name is None:
            staging_name = schema.get('mdmStagingType')
            assert staging_name is not None, f"staging_name should be given or defined in the schema."

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

    def drop_staging(self, staging_name=None, connector_id=None, connector_name=None,
                     reject_on_no_schema=False, reject_on_etl_existence=False, reject_on_mapping_existence=False
                     ):
        """

        Args:

            staging_name:  `str`,
                Staging name.
            connector_name: `str`, default `None`
                Connector name.
            connector_id: `str`, default `None`
                Connector Id.
            reject_on_no_schema: `bool` default `False`
                Do not drop if no schema found.
            reject_on_etl_existence: `bool` default `False`
                Raise an error if there exists a ETL for this staging table
            reject_on_mapping_existence: `bool` default `False`
                Raise an error if there exists a mapping for this staging table

        Returns:

            Carol response

        """

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        elif connector_id is None:
            raise ValueError("Either `connector_id` or `connector_name` should be set")

        query_string = {"connectorId" : connector_id,
                        "rejectOnNoSchema" : reject_on_no_schema,
                        "rejectOnETLExistence" : reject_on_etl_existence,
                        "rejectOnMappingExistence" : reject_on_mapping_existence}

        resp = self.carol.call_api(f'v2/staging/tables/{staging_name}/drop', method='DELETE', params=query_string)
        return resp

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

    def fetch_parquet(self, staging_name, connector_id=None, connector_name=None, backend='pandas',
                      merge_records=True, return_dask_graph=False, columns=None, max_hits=None,
                      return_metadata=False, callback=None, cds=True, max_workers=None, file_pattern=None,
                      return_callback_result=False):
        """

        Fetch parquet from a staging table.

        Args:
            staging_name: `str`,
                Staging name to fetch parquet of
            connector_id: `str`, default `None`
                Connector id to fetch parquet of
            connector_name: `str`, default `None`
                Connector name to fetch parquet of
            backend: ['dask','pandas'], default `dask`
                if to use either dask or pandas to fetch the data
            merge_records: `bool`, default `True`
                This will keep only the most recent record exported. Sometimes there are updates and/or deletions and
                one should keep only the last records.
            return_dask_graph: `bool`, default `false`
                If to return the dask graph or the dataframe.
            columns: `list`, default `None`
                List of columns to fetch.
            max_hits: `int`, default `None`
                Number of records to get. This only should be user for tests.
            return_metadata: `bool`, default `False`
                To return or not the fields like ['mdmId', 'mdmCounterForEntity', etc.]
            callback: `callable`, default `None`
                Function to be called each downloaded file.
            cds: `bool`, default `True`
                Get staging data from CDS.
            max_workers: `int` default `None`
                Number of workers to use when downloading parquet files with pandas back-end.
            file_pattern: `str` default `None`
                File pattern to filter data when fetching from CDS. e.g.
                file_pattern='2019-11-25' will fetch only CDS files that start with `2019-11-25`.
            return_callback_result `bool` default `False`
                If a callback is used, it will return the result of the response of the callback. This will skip all the
                operation to merge records and return selected columns.

        Returns: `pandas.DataFrame`
            DataFrame with the staging data.

        """
        if return_metadata:
            _meta_cols = _CAROL_METADATA_STAGING
        else:
            _meta_cols = _NEEDED_FOR_MERGE

        if callback and not callable(callback):
            raise TypeError(f'"{callback}" object is not callable')

        if backend not in ('dask','pandas'):
            raise ValueError(f"Backend options are 'dask','pandas' {backend} was given")

        if return_dask_graph and backend != 'dask':
            warnings.warn('`return_dask_graph` has no use when `backend!=dask`')

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        if columns:
            mapping_columns = columns
            columns = [i.replace("-", "_") for i in columns]
        else:
            _staging = self.get_schema(staging_name=staging_name, connector_id=connector_id)
            if not _staging:
                raise ValueError(f"{staging_name} does not exist for connector ID {connector_id}")
            mapping_columns = list(_staging['mdmStagingMapping']['properties'].keys())
            columns = [i.replace("-", "_") for i in mapping_columns]

        columns.extend(_meta_cols)
        mapping_columns = dict(zip([i.replace("-", "_") for i in columns], mapping_columns))

        # validate export
        if not cds:
            _deprecation_msgs("`cds` option was removed. Returning CDS data.")

        import_type = 'staging_cds'
        storage = Storage(self.carol)
        token_carolina = storage.backend.carolina.token
        storage_space = storage.backend.carolina.get_bucket_name(import_type)

        if backend == 'dask':
            d = _import_dask(storage=storage, connector_id=connector_id, staging_name=staging_name,
                             merge_records=merge_records, import_type=import_type, return_dask_graph=return_dask_graph,
                             mapping_columns=mapping_columns,
                             columns=columns, max_hits=max_hits)

        elif backend == 'pandas':
            import pandas as pd
            d = _import_pandas(storage=storage, connector_id=connector_id, max_workers=max_workers,
                               token_carolina=token_carolina, storage_space=storage_space,
                               staging_name=staging_name, import_type=import_type,  columns=columns,
                               max_hits=max_hits, callback=callback, mapping_columns=mapping_columns,
                               file_pattern=file_pattern)

            # TODO: Do the same for dask backend
            if d is None:
                warnings.warn(f'No data to fetch! {staging_name} has no data', UserWarning)
                cols_keys = list(self.get_schema(
                    staging_name=staging_name, connector_id=connector_id
                )['mdmStagingMapping']['properties'].keys())
                cols_keys = [i.replace("-", "_") for i in cols_keys]

                if return_metadata:
                    cols_keys.extend(_meta_cols)

                elif columns:
                    columns = [i for i in columns if i not in _meta_cols]

                d = pd.DataFrame(columns=cols_keys)
                for key, value in self.get_schema(staging_name=staging_name,
                                                  connector_id=connector_id)['mdmStagingMapping'][
                    'properties'].items():
                    key = key.replace("-", "_")
                    d.loc[:, key] = d.loc[:, key].astype(_SCHEMA_TYPES_MAPPING.get(value['type'], str), copy=False)
                if columns:
                    columns = list(set(columns))
                    d = d[list(set(columns))]
                return d.rename(columns=mapping_columns)
        else:
            raise ValueError(f'backend should be either "dask" or "pandas" you entered {backend}')

        if (return_callback_result) and (callback is not None):
            return d

        if merge_records:
            if (not return_dask_graph) or (backend == 'pandas'):
                d = drop_duplicated_parquet(d, untie_field=_CAROL_METADATA_UNTIE_STAGING)
            else:
                d = drop_duplicated_parquet_dask(d, untie_field=_CAROL_METADATA_UNTIE_STAGING)

        if not return_metadata:
            to_drop = set(_meta_cols).intersection(set(d.columns))
            d = d.drop(labels=to_drop, axis=1)

        return d

    def export(self, staging_name, connector_id=None, connector_name=None, sync_staging=True, full_export=False,
               delete_previous=False):
        """
        @DEPRECATED. This function was removed in pycarol 3.34

        Export Staging from RT to CDS

        This method will trigger or pause the export of the data in the staging to
        CDS.

        Args:
            staging_name: `str`, default `None`
                Staging name to send the data. If empty it will get from the `schema`
            connector_id: `str`, default `None`
                Connector name. If empty it will get from the `schema`
            connector_name: `str`, default `None`
                Connector name. If empty it will get from the `schema`
            sync_staging: `bool` default `True`
                Start export for this staging
            full_export: `bool` default `False`
                Full export sync for this staging
            delete_previous: `bool` default `False`
                Delete previous data.

        Returns: `None`


        Usage:
        To trigger the export the first time:

        .. code:: python


            from pycarol.staging import Staging
            from pycarol.auth.PwdAuth import PwdAuth
            from pycarol.carol import Carol
            login = Carol()
            stag  = Staging(login)
            stag.export(staging, connector_name=connector_name,sync_staging=True)

            #To do a resync, that is, start the sync from the begining without delete old data
            stag.export(staging, connector_name=connector_name,sync_staging=True, full_export=True)

            #To delete the old data:
            stag.export(staging, connector_name=connector_name,sync_staging=True, full_export=True, delete_previous=True)

            #To Pause a sync:
            stag.export(staging, connector_name=connector_name,sync_staging=False)


        """
        _deprecation_msgs("This function was removed from pyCarol")

        return None

    def export_all(self, connector_id=None, connector_name=None, sync_staging=True, full_export=False,
                   delete_previous=False):
        """

        @DEPRECATED. This function was removed in pycarol 3.34

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

        _deprecation_msgs("This function was removed from pyCarol")
        return None

    def _get_staging_export_stats(self):
        """

        @DEPRECATED. This function was removed in pycarol 3.34

        Get export status for data models

        :return: `dict`
            dict with the information of which staging table is exporting its data.
        """

        _deprecation_msgs("This function was removed from pyCarol")
        return None


    def get_mapping_snapshot(self, connector_id, mapping_id, entity_space='PRODUCTION', reverse_mapping=False):

        self.snap = {}
        querystring = {"entitySpace": entity_space, "reverseMapping": reverse_mapping}

        url = f"v1/connectors/{connector_id}/entityMappings/{mapping_id}/snapshot"
        response = self.carol.call_api(url, method='GET', params=querystring, )

        mapping_name = response.get('entityMappingName')
        return {mapping_name: response}

    def delete_mapping(self, staging_name=None, connector_id=None, connector_name=None, mapping_id=None,
                       entity_space='PRODUCTION'):

        cc = Connectors(self.carol)
        st = cc.get_dm_mappings(all_connectors=True)

        if mapping_id is None:
            assert staging_name is not None, "staging_name should be set."
            if (connector_id is None) and (connector_name is not None):
                connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']
                entity = [i['mdmId'] for i in st
                          if (i.get('mdmConnectorId') == connector_id) and
                          (i.get('mdmStagingType') == staging_name)]
                assert len(entity) == 1, f'No data model mapped for {staging_name}'
                mapping_id = entity[0]

            elif connector_id is None:
                entity = [i for i in st if (i.get('mdmStagingType') == staging_name)]

                if len(entity) > 1:
                    raise KeyError(f'There are more than one connector for staging {staging_name}')
                elif len(entity) < 1:
                    raise KeyError(f'No data model mapped for {staging_name}')
                entity = entity[0]
                connector_id = entity['mdmConnectorId']
                mapping_id = entity['mdmId']
            elif connector_id:
                entity = [i['mdmId'] for i in st
                          if (i.get('mdmConnectorId') == connector_id) and
                          (i.get('mdmStagingType') == staging_name)]

                assert len(entity) == 1, f'No mapping for {staging_name}'
                mapping_id = entity[0]

        url_mapping = f'v1/connectors/{connector_id}/entityMappings/{mapping_id}'
        querystring = {"entitySpace": entity_space, "reverseMapping": "false"}

        self.carol.call_api(url_mapping, method='DELETE', params=querystring, )

    def _check_if_exists(self, connector_id, staging_name):
        conn = Connectors(self.carol)

        try:
            mappings = conn.get_dm_mappings(connector_id=connector_id, staging_name=staging_name)
            return mappings
        except Exception as e:
            if 'Entity mapping not found' in str(e):
                return None
            else:
                raise e

    def mapping_from_snapshot(self, mapping_snapshot, connector_id=None, connector_name=None,
                              publish=True, overwrite=False):

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        staging_name = mapping_snapshot.get('stagingEntityType')
        assert staging_name, f"Snapshot incomplete, `stagingEntityType` not in snapshot"

        _mappings = self._check_if_exists(connector_id=connector_id, staging_name=staging_name)

        if (_mappings is not None) and overwrite:
            _mapping_id = [i['mdmId'] for i in _mappings]
            assert len(_mapping_id) == 1
            _mapping_id = _mapping_id[0]
            method = 'PUT'
            url_mapping = f"v1/connectors/{connector_id}/entityMappings/{_mapping_id}/snapshot"
        else:
            method = 'POST'
            url_mapping = f'v1/connectors/{connector_id}/entityMappings/snapshot'

        resp = self.carol.call_api(url_mapping, method=method, data=mapping_snapshot)
        _mapping_id = resp['mdmEntityMapping']['mdmId']

        if publish:
            url = f"v1/connectors/{connector_id}/entityMappings/{_mapping_id}/publish"
            self.carol.call_api(url, method='POST')


    def generate_SQL_tables(self, connector_id=None, connector_name=None):
        """ Generates SQL tables from a connector.

        The tables created will be named:
        `stg_{connector_id}_{staging_name}`. 

        Args:
            connector_id (str, optional): Connector ID. Defaults to None.
            connector_name (str, optional): Connector name. Defaults to None.

        Returns:
            dict: Carol's response
        

        """
        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        elif connector_id is None:
            raise ValueError("Either `connector_id` or `connector_name` should be set") 

        params = {"connectorId": connector_id}
        return self.carol.call_api(path='v3/staging/generateSqlTables', method='POST', params=params, )


    def remove_SQL_tables(self, connector_id=None, connector_name=None):
        """Remove all SQL tables from a connector

        Args:
            connector_id (str, optional): Connector ID. Defaults to None.
            connector_name (str, optional): Connector name. Defaults to None.

        Returns:
            dict: Carol's response
        

        """

        raise NotImplementedError("API not inplemented in Carol yet. Talk for support.")