import json

import time
import copy
import warnings
import asyncio

from .data_models_fields import DataModelFields
from .data_model_types import DataModelTypeIds

from ..utils.importers import _import_dask, _import_pandas
from ..verticals import Verticals
from ..storage import Storage
from ..query import delete_golden
from ..connectors import Connectors
from ..utils import async_helpers
from ..utils.miscellaneous import stream_data
from .. import _CAROL_METADATA_GOLDEN, _NEEDED_FOR_MERGE, _REJECTED_DM_COLS, _CAROL_METADATA_UNTIE_GOLDEN
from ..utils.miscellaneous import drop_duplicated_parquet, drop_duplicated_parquet_dask
from ..utils.deprecation_msgs import _deprecation_msgs
from ..exceptions import CarolApiResponseException

_DATA_MODEL_TYPES_MAPPING = {
    "boolean": bool,
    "date": str,  # TODO should it be pd.datetime?
    "long": int,
    "double": float,
    "nested": str,
    "string": str,
    "binary": str,
    "enum": str,
    "object": str,
    "geopoint": str
}


class DataModel:

    def __init__(self, carol):
        self.carol = carol
        self.fields_dict = {}
        self.entity_template_ = {}

    def get_staging_mapped(self, dm_name=None, dm_id=None):
        """
        Get mapped stagings to this data model.

        Args:
            dm_name: `str` default `None`
                Datamodel name
            dm_id: `str` default `None`
                Datamodel id

        Returns: `dict`
            A dictionary formated as:
            {
                "connector_id_1" : ["staging1", "staging2"],
                "connector_id_2" : ["staging_3", "staging_4"]
            }
        """

        if dm_name is not None:
            resp = self.get_by_name(dm_name)
            dm_id = resp['mdmId']
        elif dm_id is None:
            raise ValueError('Either `dm_name` or `dm_id` must be set.')
        url = f"v1/entities/templates/{dm_id}/mappedStagingTypes"
        query = self.carol.call_api(url, method='GET')
        return query

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": str(
                self.page_size), "sortOrder": self.sort_order}
        else:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order,
                                 "sortBy": self.sort_by}

    def _get_name_type_data_models(self, fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_data_models(
                    field['mdmFields'])
        return f

    def _get(self, id, by='id'):

        if by == 'name':
            url = f"v1/entities/templates/name/{id}"
        elif by == 'id':
            url = f"v1/entities/templates/{id}/working"
        else:
            raise print('Type incorrect, it should be "id" or "name"')

        # TODO: Add 'Not Found' and `is in Deleted state`
        resp = self.carol.call_api(url, method='GET')
        self.entity_template_ = {resp['mdmName']: resp}
        self.fields_dict.update(
            {resp['mdmName']: self._get_name_type_data_models(resp['mdmFields'])})
        return resp

    def fetch_parquet(
            self, dm_name, merge_records=True, backend='pandas',
            return_dask_graph=False,
            columns=None, return_metadata=False, callback=None,
            max_hits=None, cds=True, max_workers=None, file_pattern=None,
            return_callback_result=False
    ):
        """
        Fetch parquet from Golden.

        Args:
            dm_name: `str`
                Data model name to be imported
            merge_records: `bool`, default `True`
                This will keep only the most recent record exported. Sometimes there are updates and/or deletions and
                one should keep only the last records.
            backend: ['dask','pandas'], default `dask`
                if to use either dask or pandas to fetch the data
            return_dask_graph: `bool`, default `false`
                If to return the dask graph or the dataframe.
            columns: `list`, default `None`
                List of columns to fetch.
            return_metadata: `bool`, default `False`
                To return or not the fields like ['mdmId', 'mdmCounterForEntity', etc.]
            callback: `callable`, default `None`
                Function to be called each downloaded file.
            max_hits: `int`, default `None`
                Number of records to get.
            cds: `bool`, default `False`
                Get records from CDS.
            max_workers: `int` default `None`
                Number of workers to use when downloading parquet files with pandas back-end.
            file_pattern: `str` default `None`
                File pattern to filter data when fetching from CDS. e.g.
                file_pattern='2019-11-25' will fetch only CDS files that start with `2019-11-25`.
            return_callback_result `bool` default `False`
                If a callback is used, it will return the result of the response of the callback. This will skip all the
                operation to merge records and return selected columns.
            :return:
            """

        if backend not in ('dask', 'pandas'):
            raise ValueError(
                f"Backend options are 'dask','pandas' {backend} was given")

        if return_metadata:
            # It can be costly to get all meta from a golden. So er should alway ask for the info we want.
            _meta_cols = _CAROL_METADATA_GOLDEN
        else:
            _meta_cols = _NEEDED_FOR_MERGE

        if callback and not callable(callback):
            raise TypeError(f'"{callback}" object is not callable')

        all_cols = list(self._get_name_type_DMs(
            self.get_by_name(dm_name)['mdmFields']))
        if not columns:  # if an empty list was sent.
            columns = all_cols

        elif isinstance(columns, str):
            columns = [columns]

        _diff_cols = set(columns) - set(all_cols)
        if len(_diff_cols) > 0:
            warnings.warn(
                f"It seems there was used columns not in this data model: {_diff_cols}", UserWarning)

        if return_dask_graph and backend != 'dask':
            warnings.warn(
                '`return_dask_graph` has no use when `backend!=dask`')

        if not cds:
            _deprecation_msgs("`cds` option will be removed from pycarol 3.33. Consider use `cds=True`"
                              " to avoid problems. ")

        import_type = 'golden_cds'
        if columns:
            columns.extend(_meta_cols)

        storage = Storage(self.carol)
        token_carolina = storage.backend.carolina.token
        storage_space = storage.backend.carolina.get_bucket_name(import_type)

        if backend == 'dask':
            d = _import_dask(storage=storage, dm_name=dm_name,
                             import_type=import_type,
                             merge_records=merge_records,
                             return_dask_graph=return_dask_graph,
                             columns=columns)

        elif backend == 'pandas':
            import pandas as pd
            d = _import_pandas(storage=storage, dm_name=dm_name,
                               import_type=import_type, columns=columns,
                               callback=callback, max_hits=max_hits,
                               max_workers=max_workers,
                               token_carolina=token_carolina,
                               storage_space=storage_space, file_pattern=file_pattern)
            if d is None:
                warnings.warn("No data to fetch!", UserWarning)
                _field_types = self._get_name_type_DMs(
                    self.get_by_name(dm_name)['mdmFields'])
                cols_keys = list(_field_types)
                if return_metadata:
                    cols_keys.extend(_meta_cols)

                elif columns:
                    columns = [i for i in columns if i not in _meta_cols]

                d = pd.DataFrame(columns=cols_keys)
                for key, value in _field_types.items():
                    if isinstance(value, dict):
                        value = "STRING"  # If nested we receive as a `STR`
                    d.loc[:, key] = d.loc[:, key].astype(
                        _DATA_MODEL_TYPES_MAPPING.get(value.lower(), str), copy=False)
                if columns:
                    columns = list(set(columns))
                    d = d[list(set(columns))]
                return d

        else:
            raise ValueError(
                f'backend should be either "dask" or "pandas" you entered {backend}')

        if (return_callback_result) and (callback is not None):
            return d

        if merge_records:
            if (not return_dask_graph) or (backend == 'pandas'):
                d = drop_duplicated_parquet(
                    d, untie_field=_CAROL_METADATA_UNTIE_GOLDEN)
            else:
                d = drop_duplicated_parquet_dask(
                    d, untie_field=_CAROL_METADATA_UNTIE_GOLDEN)

        if not return_metadata:
            to_drop = set(_meta_cols).intersection(set(d.columns))
            d = d.drop(labels=to_drop, axis=1)

        return d

    def get_all(self, offset=0, page_size=-1, sort_order='ASC',
                sort_by=None, print_status=False,
                save_file=None, only_published=False):
        """ 
        Fetch all datamodels definition. 

        Args:

            offset: `int`, default 0
                Offset for pagination. Only used when `scrollable=False`
            page_size: `int`, default 100
                Number of records downloaded in each pagination. The maximum value is 1000
            sort_order: `str`, default 'ASC'
                Sort ascending ('ASC') vs. descending ('DESC').
            sort_by: `str`,  default `None`
                Name to sort by.
            print_status: `bool`, default False
                Print number of records donwloaded. 
            save_file: `str`, default `None`
                If not `None` the results will be saved. 
            only_published: `bool`, default `False`
                Get only published data models. 


        """

        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.sort_by = sort_by
        self._build_query_params()

        self.template_dict = {}
        self.template_data = []
        count = self.offset

        set_param = True
        self.total_hits = float("inf")
        if save_file:
            assert isinstance(save_file, str)
            file = open(save_file, 'w', encoding='utf8')

        if only_published:
            url_filter = "v1/entities/templates/published"
        else:
            url_filter = "v1/entities/templates"

        while count < self.total_hits:

            query = self.carol.call_api(
                url_filter, params=self.query_params, method='GET')

            if query['count'] == 0:
                print('There are no more results.')
                print('Expecting {}, reponse = {}'.format(
                    self.total_hits, count))
                break
            count += query['count']
            if set_param:
                self.total_hits = query["totalHits"]
                set_param = False

            query = query['hits']
            self.template_data.extend(query)
            self.fields_dict.update({i['mdmName']: self._get_name_type_data_models(i['mdmFields'])
                                     for i in query})
            self.template_dict.update({i['mdmName']: {'mdmId': i['mdmId'],
                                                      'mdmEntitySpace': i['mdmEntitySpace'],
                                                      'mdmPublishedExists': i['mdmPublishedExists']}
                                       for i in query})

            self.query_params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.total_hits), end='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self

    def get_by_name(self, name):
        return self._get(name, by='name')

    def get_by_id(self, id):
        return self._get(id, by='id')

    def get_snapshot(self, dm_id, entity_space):
        url_snapshot = f'v1/entities/templates/{dm_id}/snapshot?entitySpace={entity_space}'
        resp = self.carol.call_api(url_snapshot, method='GET')
        self.snapshot_ = {resp['entityTemplateName']: resp}
        return resp

    def export(self, dm_name=None, dm_id=None, sync_dm=True, full_export=False, delete_previous=False):
        """

        @DEPRECATED. This function was removed in pycarol 3.34

        Export data models

        This method will trigger or pause the export of the data in the datamodel to
        CDS

        :param dm_name: `str`, default `None`
            Data model Name
        :param dm_id: `str`, default `None`
            Data model id
        :param sync_dm: `bool`, default `True`
            Sync the data model
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None
        """
        _deprecation_msgs("This function was removed from pyCarol")
        return None

    def export_all(self, sync_dm=True, full_export=False, delete_previous=False):
        """

        @DEPRECATED. This function was removed in pycarol 3.34

        Export all data models

        This method will trigger or pause the export of the data in the datamodel to
        CDS

        :param sync_dm: `bool`, default `True`
            Sync the data model
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None
        """

        _deprecation_msgs("This function was removed from pyCarol")
        return None

    def delete(self, dm_id=None, dm_name=None, entity_space='WORKING'):
        # TODO: Check Possible entity_spaces

        if dm_id is None:
            assert dm_name is not None
            resp = self.get_by_name(dm_name)
            dm_id = resp['mdmId']

        url = f"v1/entities/templates/{dm_id}"
        querystring = {"entitySpace": entity_space}

        return self.carol.call_api(url, method='DELETE', params=querystring)

    def _get_name_type_DMs(self, fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_DMs(
                    field['mdmFields'])
        return f

    def _get_dm_export_stats(self):
        """

        @DEPRECATED. This function was removed in pycarol 3.34

        Get export status for data models

        :return: `dict`
            dict with the information of which data model is exporting its data.
        """

        _deprecation_msgs("This function was removed from pyCarol")
        return None

    def reprocess_rejected(self, dm_name=None, dm_id=None, delete_records=True):
        """
        Reprocess rejected records.

        Args:
            dm_name: `str` default None
                 Data model name
            dm_id: `str` default None
                 Data model ID
            delete_records: `boll` default True
                True to delete and reprocess, False for copy and reprocess.

        Returns: dict
            Task Definition.
        """

        if dm_name is None and dm_id is None:
            raise ValueError('Either dm_name or dm_id must be set.')

        dm_id = dm_id if dm_id else self.get_by_name(dm_name)['mdmId']

        params = {
            'recordType': 'REJECTED',
            'fuzzy': False,
            'queryDescription': {"a": "REJECTED", "q": "*", "p": "1", "o": "ASC", "r": "[]"},
            'deleteRecords': delete_records
        }
        result = self.carol.call_api(
            f'v1/entities/templates/{dm_id}/reprocess', method='POST', params=params)

        return result

    def reprocess(self, dm_name, copy_or_move='move', record_type='ALL', query_filter=None):
        """
        Reprocess records from a data model

        :param dm_name: `str`
            Data model name
        :param copy_or_move: `str`, default `move`
            Either `move` or `copy` to staging.
        :param record_type:  `str`, default `ALL`
            Type of records to reprocess. `All`, `Golden` or `Rejected`
        :param query_filter:  `Filter.Builder Object`, default `None`
            The Filter instance to reprocess the data on.
        :return: None
        """

        if copy_or_move not in ["copy", "move"]:
            ValueError(
                f'copy_or_move should be "copy" or "move". {copy_or_move} was used')

        if copy_or_move == 'copy':
            copy_or_move = False
        else:
            copy_or_move = True

        mdm_id = self.get_by_name(dm_name)['mdmId']

        url_filter = f"v1/entities/templates/{mdm_id}/reprocess"

        query_params = {"recordType": record_type, "fuzzy": "false",
                        "deleteRecords": copy_or_move}

        result = self.carol.call_api(
            url_filter, data=query_filter, params=query_params, method='POST')
        return result

    def send_data(self, data, dm_name=None, dm_id=None, step_size=500, gzip=False, delete_old_records=False,
                  print_stats=True, max_workers=2, async_send=False):
        """
        :param data: pandas data frame, json.
            Data to be send to Carol
        :param dm_name:  `str`, default `None`
            Data model name
        :param dm_id:  `str`, default `None`
            Data model id
        :param step_size: `int`, default `500`
            Number of records to be sent in each iteration. Max size for each batch is 10MB
        :param print_stats: `bool`, default `True`
            If print the status
        :param gzip: `bool`, default `True`
            If send each batch as a gzip file.
        :param delete_old_records: `bool`, default `False`
            Delete previous records in the data model.
        :param max_workers: `int`, default `2`
            To be used with `async_send=True`. Number of threads to use when sending.
        :param async_send: `bool`, default `False`
            To use async to send the data. This is much faster than a sequential send.
        :return: None
        """

        self.gzip = gzip
        extra_headers = {}
        content_type = 'application/json'
        if self.gzip:
            content_type = None
            extra_headers["Content-Encoding"] = "gzip"
            extra_headers['content-type'] = 'application/json'

        if dm_name:
            dm_id = self.get_by_name(dm_name)['mdmId']
        else:
            assert dm_id
            dm_name = self._get(dm_id, by='id')['mdmName']

        if delete_old_records:
            delete_golden(self.carol, dm_name)

        is_df = False
        if isinstance(data, str):
            data = json.loads(data)
            data_size = len(data)
            _sample_json = data[0]
        elif isinstance(data, list):
            data_size = len(data)
            _sample_json = data[0]
        else:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                is_df = True
                data_size = data.shape[0]
                _sample_json = data.iloc[0].to_json(date_format='iso')

        if (not isinstance(data, list)) and (not is_df):
            data = [data]
            data_size = len(data)

        url = f"v1/entities/templates/{dm_id}/goldenRecords?async=true"

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

                self.carol.call_api(url, data=data_json, extra_headers=extra_headers,
                                    content_type=content_type, status_forcelist=[
                                        502, 429, 502],
                                    method_whitelist=frozenset(['POST']))
                if print_stats:
                    print('{}/{} sent'.format(cont, data_size), end='\r')

    def create_mapping(self, staging_name, connector_id=None, connector_name=None, dm_name=None, dm_id=None,
                       publish=False):
        """

        This method will create the link between the staging and a data model. If Publish=True it will publish how it is.

        :param staging_name:  `str`
            Name of the staging to be mapped.
        :param connector_id:  `str`, `str`, default `None`
            Staging's connector ID
        :param connector_name:  `str`, default `None`
            Staging's connector name
        :param dm_name: `int`, default `100`
            Data Model name
        :param dm_id: `bool`, default `True`
            Data model ID
        :param publish: `bool`, default `True`
            If publish after create mapping.
        :return: None
        """

        if connector_name:
            _con = Connectors(self.carol)
            connector_id = _con.get_by_name(connector_name)['mdmId']
        else:
            assert connector_id
            _con = Connectors(self.carol)
            connector_name = _con.get_by_id(connector_id)['mdmName']

        if dm_name:
            dm_id = self.get_by_name(dm_name)['mdmId']
        else:
            assert dm_id
            dm_name = self.get_by_id(dm_id)['mdmName']

        url = f"v1/connectors/{connector_id}/entityMappings"

        payload = {"mdmMasterEntityId": dm_id,
                   "mdmMasterEntityName": dm_name,
                   "mdmConnectorId": connector_id,
                   "mdmStagingType": staging_name
                   }
        resp = self.carol.call_api(url, data=payload)
        if publish:
            url = f"v1/connectors/{connector_id}/entityMappings/{resp['mdmId']}/publish"

            self.carol.call_api(url, method='POST')

        return resp

    def fetch_rejected(
            self, dm_name, backend='pandas',
            return_dask_graph=False, callback=None,
            max_hits=None, max_workers=None, file_pattern=None,
            return_callback_result=False
    ):
        """
        Fetch rejected records from Golden.

            Args:
                dm_name: `str`
                    Data model name to be imported
                backend: ['dask','pandas'], default `dask`
                    if to use either dask or pandas to fetch the data
                return_dask_graph: `bool`, default `false`
                    If to return the dask graph or the dataframe.
                callback: `callable`, default `None`
                    Function to be called each downloaded file.
                max_hits: `int`, default `None`
                    Number of records to get.
                max_workers: `int` default `None`
                    Number of workers to use when downloading parquet files with pandas back-end.
                file_pattern: `str` default `None`
                    File pattern to filter data when fetching from CDS. It is possible to fetch only records from a staging table. e.g.
                    file_pattern='{connector_id}_{staging_name}/2019-11-25' will fetch only CDS files that came from
                    the connector  "connector_id" and staging "staging_name" on "2019-11-25".
                return_callback_result `bool` default `False`
                    If a callback is used, it will return the result of the response of the callback. This will skip all the
                    operation to merge records and return selected columns.
                :return:
                    pd.DataFrame with rejected records.
                """

        if backend not in ('dask', 'pandas'):
            raise ValueError(
                f"Backend options are 'dask','pandas' {backend} was given")

        if callback and not callable(callback):
            raise TypeError(f'"{callback}" object is not callable')

        if return_dask_graph and backend != 'dask':
            warnings.warn(
                '`return_dask_graph` has no use when `backend!=dask`')

        import_type = 'golden_rejected'

        storage = Storage(self.carol)
        token_carolina = storage.backend.carolina.token
        storage_space = storage.backend.carolina.get_bucket_name(import_type)

        if backend == 'dask':
            d = _import_dask(
                storage=storage, dm_name=dm_name,
                import_type=import_type,
                merge_records=False,
                return_dask_graph=return_dask_graph,
                columns=None, file_pattern=file_pattern,
            )

        elif backend == 'pandas':
            import pandas as pd
            d = _import_pandas(
                storage=storage, dm_name=dm_name,
                import_type=import_type, columns=None,
                callback=callback, max_hits=max_hits,
                max_workers=max_workers,
                token_carolina=token_carolina,
                storage_space=storage_space, file_pattern=file_pattern
            )
            if d is None:
                warnings.warn("No data to fetch!", UserWarning)
                _field_types = self._get_name_type_DMs(
                    self.get_by_name(dm_name)['mdmFields'])
                cols_keys = list(_field_types)

                d = pd.DataFrame(columns=cols_keys)
                for key, value in _field_types.items():
                    if isinstance(value, dict):
                        value = "STRING"  # If nested we receive as a `STR`
                    d.loc[:, key] = d.loc[:, key].astype(
                        _DATA_MODEL_TYPES_MAPPING.get(value.lower(), str), copy=False)
                return d

        else:
            raise ValueError(
                f'backend should be either "dask" or "pandas" you entered {backend}')

        if (return_callback_result) and (callback is not None):
            return d

        return d

    def generate_SQL_tables(self,):
        """Geneates SQL tables for all data models. 

        The tables created will be named:
        `dm_{datamodel_name}`. 

        Returns:
            dict: Carol's response
        """

        return self.carol.call_api(path='v1/admin/entities/templates/generateSqlTables', method='POST')

    def remove_SQL_tables(self,):
        """Removes SQL tables for all data models. 

        Returns:
            dict: Carol's response
        """

        return self.carol.call_api(path='v1/admin/entities/templates/removeSqlTables', method='POST') 


class entIntType(object):
    ent_type = 'long'


class entDoubleType(object):
    ent_type = 'double'


class entStringType(object):
    ent_type = "string"


class entNullType(object):
    ent_type = "string"


class entBooleanType(object):
    ent_type = "boolean"


class entArrayType(object):
    ent_type = "nested"


class entObjectType(object):
    ent_type = "object"


class entType(object):
    @classmethod
    def get_ent_type_for(cls, t):
        """docstring for get_schema_type_for"""
        SCHEMA_TYPES = {
            type(None): entNullType,
            str: entStringType,
            int: entIntType,
            float: entDoubleType,
            bool: entBooleanType,
            list: entArrayType,
            dict: entObjectType,
        }

        schema_type = SCHEMA_TYPES.get(t)

        if not schema_type:
            raise JsonEntTypeNotFound("There is no schema type for  %s.\n Try:\n %s" % (
                str(t), ",\n".join(["\t%s" % str(k) for k in SCHEMA_TYPES.keys()])))
        return schema_type


class JsonEntTypeNotFound(Exception):
    pass


class CreateDataModel(object):
    def __init__(self, carol):
        self.carol = carol
        self.template_dict = {}

        self.fields = DataModelFields(self.carol)
        self.fields.possible_types()
        self.all_possible_types = self.fields._possible_types
        self.all_possible_fields = self.fields.fields_dict

    def from_snapshot(self, snapshot, publish=False, overwrite=False):

        _count = 0

        while True:
            url = 'v1/entities/templates/snapshot'
            resp = self.carol.call_api(
                path=url, method='POST', data=snapshot, errors='ignore')

            if ('already exists' in resp.get('errorMessage', 'asdf')) and (overwrite):
                del_DM = DataModel(self.carol)
                del_DM.get_by_name(snapshot['entityTemplateName'])
                dm_id = del_DM.entity_template_.get(
                    snapshot['entityTemplateName']).get('mdmId', None)
                if dm_id is None:  # if None
                    continue
                entity_space = del_DM.entity_template_.get(
                    snapshot['entityTemplateName'])['mdmEntitySpace']
                del_DM.delete(dm_id=dm_id, entity_space=entity_space)
                time.sleep(0.5)  # waint for deletion
                _count += 1
                if _count > 5:
                    print(
                        f"Something wrong coping {snapshot['entityTemplateName']}")
                    print(f"Data model was not copied: {resp}")
                    return
                continue

            break
        print('Data Model {} created'.format(snapshot['entityTemplateName']))
        self.template_dict.update({resp['mdmName']: resp})
        if publish:
            self.publish_template(resp['mdmId'])

    def publish_template(self, dm_id):
        url = f'v1/entities/templates/{dm_id}/publish'
        resp = self.carol.call_api(path=url, method='POST')
        return resp

    def _check_verticals(self):
        self.verticals_dict = Verticals(self.carol).all()

        if self.vertical_ids is not None:
            for key, value in self.verticals_dict.items():
                if value == self.vertical_ids:
                    self.vertical_ids = value
                    self.vertical_names = key
                    return
        else:
            for key, value in self.verticals_dict.items():
                if key == self.vertical_names:
                    self.vertical_ids = value
                    self.vertical_names = key
                    return

        raise Exception('{}/{} are not valid values for mdmVerticalNames/mdmVerticalIds./n'
                        ' Possible values are: {}'.format(self.vertical_names, self.vertical_ids,
                                                          self.verticals_dict))

    def _check_entity_template_types(self):
        self.template_type_dict = DataModelTypeIds(self.carol).all()

        if self.entity_template_type_ids is not None:
            for key, value in self.template_type_dict.items():
                if value == self.entity_template_type_ids:
                    self.entity_template_type_ids = value
                    self.entity_template_type_names = key
                    return
        else:
            for key, value in self.template_type_dict.items():
                if key == self.entity_template_type_names:
                    self.entity_template_type_ids = value
                    self.entity_template_type_names = key
                    return

        raise Exception('{}/{} are not valid values for mdmEntityTemplateTypeNames/mdmEntityTemplateTypeIds./n'
                        ' Possible values are: {}'.format(self.vertical_names, self.vertical_ids,
                                                          self.template_type_dict))

    def _check_dm_name(self):

        est_ = DataModel(self.carol)
        try:
            est_.get_by_name(self.dm_name)
        except CarolApiResponseException:
            return

        raise Exception('mdm name {} already exist'.format(self.dm_name))

    def create(self, dm_name, overwrite=False, vertical_ids=None, vertical_names=None, entity_template_type_ids=None,
               entity_template_type_names=None, label=None, group_name='Others',
               transaction_data_model=False):

        self.dm_name = dm_name
        self.group_name = group_name

        if not label:
            self.label = self.dm_name
        else:
            self.label = label
        self.transaction_data_model = transaction_data_model

        assert ((vertical_names is not None) or (vertical_ids is not None))
        assert ((entity_template_type_ids is not None)
                or (entity_template_type_names is not None))

        self.vertical_names = vertical_names
        self.vertical_ids = vertical_ids
        self.entity_template_type_ids = entity_template_type_ids
        self.entity_template_type_names = entity_template_type_names

        self._check_verticals()
        self._check_entity_template_types()
        if not overwrite:
            self._check_dm_name()

        payload = {"mdmName": self.dm_name, "mdmGroupName": self.group_name, "mdmLabel": {"en-US": self.label},
                   "mdmVerticalIds": [self.vertical_ids],
                   "mdmEntityTemplateTypeIds": [self.entity_template_type_ids],
                   "mdmTransactionDataModel": self.transaction_data_model, "mdmProfileTitleFields": []}

        while True:
            url_filter = "v1/entities/templates"
            resp = self.carol.call_api(
                url_filter, data=payload, method='POST', errors='ignore')
            # error handler for token
            if ('already exists' in resp.get('errorMessage', [])) and (overwrite):
                del_DM = DataModel(self.carol)
                del_DM.get_by_name(self.dm_name)
                dm_id = del_DM.entity_template_.get(
                    self.dm_name).get('mdmId', None)
                if dm_id is None:  # if None
                    continue
                entity_space = del_DM.entity_template_.get(self.dm_name)[
                    'mdmEntitySpace']
                del_DM.delete(dm_id=dm_id, entity_space=entity_space)
                time.sleep(0.5)  # waint for deletion
                continue
            break

        self.template_dict.update({resp['mdmName']: resp})

    def _profile_title(self, profile_title, dm_id):
        if isinstance(profile_title, str):
            profile_title = [profile_title]

        profile_title = [i.lower() for i in profile_title]

        url = f"v1/entities/templates/{dm_id}/profileTitle"
        resp = self.carol.call_api(path=url, method='POST', data=profile_title)
        return resp

    def add_field(self, field_name, dm_id=None, parent_field_id=""):

        if dm_id is None:
            assert self.dm_id
        else:
            est_ = DataModel(self.carol)
            est_.get_by_id(dm_id)
            if est_.entity_template_ == {}:
                print('Template does not exisit')
                return
            self.dm_id = dm_id
            _, template_ = est_.entity_template_.popitem()
            self.current_fields = [
                i for i in template_['mdmFieldsFull'].keys()]
            if field_name.lower() in self.current_fields:
                print("'{}' already in the template".format(field_name))
                return

        field_to_send = self.all_possible_fields.get(field_name)
        if field_to_send is None:
            print('Field does not exist')
            return
        querystring = {"parentFieldId": parent_field_id}

        url = f"v1/entities/templates/{self.dm_id}/onboardField/{field_to_send['mdmId']}"
        resp = self.carol.call_api(path=url, method='POST', params=querystring)

    def _labels_and_desc(self, prop):

        if self.label_map is None:
            label = prop
        else:
            label = self.label_map.get(prop)
            if label is None:
                label = prop
            else:
                label = {"en-US": label}

        if self.description_map is None:
            description = prop
        else:
            description = self.description_map.get(prop)
            if description is None:
                description = prop
            else:
                description = {"en-US": description}

        return label, description

    def from_json(self, json_sample, profile_title=None, publish=False, dm_id=None,
                  label_map=None, description_map=None, ignore_field_type=False):

        if publish:
            assert profile_title is not None, "To publish the data model, `profile_title` has to be set."
            if isinstance(profile_title, str):
                profile_title = [profile_title]
            assert all([i in json_sample for i in profile_title]
                       ), "all profile title values should be in `json_sample`"

        self.label_map = label_map
        self.description_map = description_map

        if dm_id is None:
            assert self.template_dict is not None
            template_name, template_json = self.template_dict.copy().popitem()
            self.dm_id = template_json['mdmId']

        else:
            self.dm_id = dm_id

        self.json_sample = json_sample

        n_fields = len(list(self.json_sample))
        count = 0
        for prop, value in self.json_sample.items():
            count += 1
            print('Creating {}/{}'.format(count, n_fields))
            prop = prop.lower()
            entity_type = entType.get_ent_type_for(type(value))
            if prop in self.all_possible_fields.keys():
                if not entity_type.ent_type == 'nested':
                    ent_ = self.all_possible_fields.get(prop, []).copy()
                    ent_.pop('mdmCreated')
                    ent_.pop('mdmLastUpdated')
                    ent_.pop('mdmTenantId')
                    if (ent_['mdmMappingDataType'].lower() == entity_type.ent_type) or (ignore_field_type):
                        self.add_field(prop, parent_field_id="")
                    else:
                        print('problem, {} not created, field name matches with an already'
                              'created field but different type'.format(prop))
                else:
                    print('Nested fields are not supported')
            else:
                if not entity_type.ent_type == 'nested':

                    current_label, current_description = self._labels_and_desc(
                        prop)
                    self.fields.create(mdm_name=prop, mdm_mpping_data_type=entity_type.ent_type,
                                       mdm_field_type='PRIMITIVE', admin=True,
                                       mdm_label=current_label, mdm_description=current_description)
                    self.all_possible_fields = self.fields.fields_dict
                    self.add_field(prop, parent_field_id="")
                else:
                    print('Nested fields are not supported')

        if publish:
            self._profile_title(profile_title, self.dm_id)
            self.publish_template(self.dm_id)
