from .schema_generator import carolSchemaGenerator
import pandas as pd
import json
from .query import Query
from datetime import datetime
from .connectors import Connectors
from .carolina import Carolina
from .utils.importers import _import_dask, _import_pandas
from .filter import Filter, RANGE_FILTER, TYPE_FILTER
import itertools



class Staging:
    def __init__(self, carol):
        self.carol = carol


    def _delete(self,dm_name):

        now = datetime.now().isoformat(timespec='seconds')

        json_query = Filter.Builder()\
            .type(dm_name + "Golden")\
            .must(RANGE_FILTER("mdmLastUpdated", [None, now]))\
            .build().to_json()


        try:
            Query(self.carol).delete(json_query)
        except:
            pass

        json_query = Filter.Builder()\
            .type(dm_name + "Rejected")\
            .must(RANGE_FILTER("mdmLastUpdated", [None, now]))\
            .build().to_json()

        try:
            Query(self.carol,index_type='STAGING').delete(json_query)
        except:
            pass



    def send_data(self, staging_name, data=None, connector_id=None, step_size=100, print_stats=False,
                  auto_create_schema=False, crosswalk_auto_create=None, force=False, dm_to_delete=None):

        if connector_id is None:
            connector_id = self.carol.connector_id

        schema = self.get_schema(staging_name,connector_id)

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
            self.create_schema(_sample_json, staging_name, connector_id=connector_id, crosswalk_list=crosswalk_auto_create)
            _crosswalk = crosswalk_auto_create
            print('provided crosswalk ',_crosswalk)
        elif auto_create_schema:
            assert crosswalk_auto_create, "You should provide a crosswalk"
            self.create_schema(_sample_json, staging_name, connector_id=connector_id,
                               crosswalk_list=crosswalk_auto_create, overwrite=True)
            _crosswalk = crosswalk_auto_create
            print('provided crosswalk ',_crosswalk)
        else:
            _crosswalk = schema["mdmCrosswalkTemplate"]["mdmCrossreference"].values()
            _crosswalk = list(_crosswalk)[0]
            print('fetched crosswalk ',_crosswalk)

        if is_df and not force:
            assert data.duplicated(subset=_crosswalk).sum() == 0, \
                "crosswalk is not unique on dataframe. set force=True to send it anyway."

        if dm_to_delete is not None:
            self._delete(dm_to_delete)


        url = f'v2/staging/tables/{staging_name}?returnData=false&connectorId={connector_id}'
        gen = self._stream_data(data, data_size, step_size, is_df)
        cont = 0
        ite = True
        data_json = gen.__next__()
        while ite:
            self.carol.call_api(url, data=data_json)
            cont += len(data_json)
            if print_stats:
                print('{}/{} sent'.format(cont, data_size), end='\r')
            data_json = gen.__next__()
            if data_json == []:
                break

    def _stream_data(self, data, data_size, step_size, is_df):
        for i in range(0,data_size, step_size):
            if is_df:
                data_to_send = data.iloc[i:i + step_size].to_json(orient='records', date_format='iso', lines=False)
                data_to_send = json.loads(data_to_send)
                yield data_to_send
            else:
                yield data[i:i + step_size]

        yield []

    def get_schema(self, staging_name, connector_id=None):
        query_string = None
        if connector_id:
            query_string = {"connectorId": connector_id}
        try:
            return self.carol.call_api(f'v2/staging/tables/{staging_name}/schema',  method='GET',
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
                                    crosswalkname=crosswalk_name,crosswalkList=crosswalk_list)
        elif isinstance(fields_dict, str):
            schema = carolSchemaGenerator.from_json(fields_dict)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name, crosswalkList=crosswalk_list)
        else:
            print('Behavior for type %s not defined!' % type(fields_dict))

        query_string = {"connectorId": connector_id}
        if connector_id is None:
            connector_id = self.carol.connector_id
            query_string = {"connectorId": connector_id}

        has_schema = self.get_schema(staging_name,connector_id=connector_id) is not None
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

    def fetch_parquet(self, staging_name, connector_id=None, connector_name=None, backend='dask',verbose=0,
                      merge_records=True, n_jobs=1, return_dask_graph=False, columns=None):
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
        :return:
        """


        if columns:
            old_columns = columns
            columns = [i.replace("-","_") for i in columns]
            columns.extend(['mdmId', 'mdmCounterForEntity'])
            old_columns = dict(zip([i.replace("-", "_") for i in columns], old_columns))
        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        assert backend=='dask' or backend=='pandas'

        if return_dask_graph:
            assert backend == 'dask'

        #validate export
        stags = self._get_staging_export_stats()
        if not stags.get(staging_name):
            raise Exception(f'"{staging_name}" is not set to export data, \n use `dm = Staging(login).export(staging_name="{staging_name}",connector_id="{connector_id}", sync_staging=True) to activate')

        carolina = Carolina(self.carol)
        carolina._init_if_needed()
        if backend=='dask':
            access_id = carolina.ai_access_key_id
            access_key = carolina.ai_secret_key
            aws_session_token = carolina.ai_access_token

            d = _import_dask(tenant_id=self.carol.tenant['mdmId'], connector_id=connector_id, staging_name=staging_name,
                             access_key=access_key, access_id=access_id, aws_session_token=aws_session_token,
                             merge_records=merge_records, golden=False,return_dask_graph=return_dask_graph,columns=columns)

        elif backend=='pandas':
            s3 = carolina.s3
            d = _import_pandas(s3=s3,  tenant_id=self.carol.tenant['mdmId'], connector_id=connector_id, verbose=verbose,
                               staging_name=staging_name, n_jobs=n_jobs, golden=False, columns=columns)
        else:
            raise ValueError(f'backend should be "dask" or "pandas" you entered {backend}' )

        if merge_records:
            if not return_dask_graph:
                d.rename(columns=old_columns, inplace=True)
                d.sort_values('mdmCounterForEntity', inplace=True)
                d.reset_index(inplace=True, drop=True)
                d.drop_duplicates(subset='mdmId', keep='last', inplace=True)
                d.reset_index(inplace=True, drop=True)
            else:
                d.rename(columns=old_columns, inplace=True)
                d = d.set_index('mdmCounterForEntity', sorted=True) \
                     .drop_duplicates(subset='mdmId', keep='last') \
                     .reset_index(drop=True)

        return d


    def export(self,staging_name, connector_id=None, connector_name=None, sync_staging=True, full_export=False):
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
        :return: None
        """

        if sync_staging:
            status = 'RUNNING'
        else:
            status = 'PAUSED'

        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id


        query_params = {"status": status, "fullExport": full_export}
        url = f'v2/staging/{connector_id}/{staging_name}/exporter'
        return self.carol.call_api(url, method='POST', params=query_params)


    def export_all(self, connector_id=None, connector_name=None, sync_staging=True, full_export=False):
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
        :return: None
        """
        if connector_name:
            connector_id = self._connector_by_name(connector_name)
        else:
            assert connector_id

        conn_stats = Connectors(self.carol).stats(connector_id=connector_id)

        for staging in conn_stats.get(connector_id):
            resp = self.export(staging_name=staging, connector_id=connector_id,
                        sync_staging=sync_staging, full_export=full_export )

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