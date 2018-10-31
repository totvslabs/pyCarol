from .schema_generator import carolSchemaGenerator
import pandas as pd
import json
from .query import Query
from datetime import datetime
from .connectors import Connectors


class Staging:
    def __init__(self, carol):
        self.carol = carol


    def _delete(self,dm_name):

        now = datetime.now().isoformat(timespec='seconds')
        json_query = {
                      "mustList": [
                        {
                          "mdmFilterType": "TYPE_FILTER",
                          "mdmValue": dm_name+'Golden'
                        },
                        {
                          "mdmFilterType": "RANGE_FILTER",
                          "mdmKey": "mdmLastUpdated",
                          "mdmValue": [
                            None,
                            now
                          ]
                        }
                      ]
                    }
        try:
            Query(self.carol).delete(json_query)
        except:
            pass

        json_query = {
                      "mustList": [
                        {
                          "mdmFilterType": "TYPE_FILTER",
                          "mdmValue": dm_name+'Rejected'
                        },
                        {
                          "mdmFilterType": "RANGE_FILTER",
                          "mdmKey": "mdmLastUpdated",
                          "mdmValue": [
                            None,
                            now
                          ]
                        }
                      ]
                    }
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
        return Connectors(self.carol).get_by_name(connector_name)['mdmId']


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


    def export_all(self, connector_id=None, connector_name=None, sync_staging=True, full_export=False, verbose=False ):
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

            if verbose:
                print(resp)


        pass
