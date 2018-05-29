from .schemaGenerator import *
import pandas as pd


class Staging:
    def __init__(self, carol):
        self.carol = carol

    def send_data(self, staging_name, data=None, step_size=100, print_stats=False, auto_create_schema=True):
        schema = self.get_schema(staging_name)

        if not schema and auto_create_schema:
            self.create_schema(data, staging_name)

        is_df = False
        if isinstance(data, pd.DataFrame):
            is_df = True
            data_size = data.shape[0]
        elif isinstance(data, str):
            data = json.loads(data)
            data_size = len(data)
        else:
            data_size = len(data)

        if (not isinstance(data, list)) and (not is_df):
            data = [data]
            data_size = len(data)

        url = 'v2/staging/tables/{}?returnData=false'.format(staging_name)

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
                data = data.iloc[i:i + step_size].to_json(orient='records', date_format='iso', lines=False)
                data = json.loads(data)
                yield data
            else:
                yield data[i:i + step_size]

        yield []

    def get_schema(self, staging_name):
        return self.carol.call_api('v2/staging/tables/{}/schema'.format(staging_name))

    def create_schema(self, fields_dict, staging_name, mdm_flexible='false',
                     crosswalk_name=None, crosswalk_list=None, overwrite=False):
        assert fields_dict is not None

        if isinstance(fields_dict, dict):
            schema = carolSchemaGenerator(fields_dict)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name,crosswalkList=crosswalk_list)
        elif isinstance(fields_dict, str):
            schema = carolSchemaGenerator.from_json(fields_dict)
            schema = schema.to_dict(mdmStagingType=staging_name, mdmFlexible=mdm_flexible,
                                    crosswalkname=crosswalk_name, crosswalkList=crosswalk_list)

        has_schema = self.get_schema(staging_name) is not None
        if has_schema:
            method = 'PUT'
        else:
            method = 'POST'

        resp = self.carol.call_api('v2/staging/tables/{}/schema'.format(staging_name), data=schema, method=method)
        if resp['success']:
            print('Schema sent successfully!')
        else:
            print('Failed to send schema: ' + resp)