import json
import itertools
from .data_models_fields import DataModelFields
from .data_model_types import DataModelTypeIds
from .utils.importers import _import_dask, _import_pandas
from .verticals import Verticals
from .carolina import Carolina
from .query import Query
from .filter import RANGE_FILTER as RF
from .filter import TYPE_FILTER, Filter, MAXIMUM, MINIMUM
from .utils.miscellaneous import ranges
import time
import copy
import warnings
import pandas as pd
import gzip, io
import asyncio
from concurrent.futures import ThreadPoolExecutor


class DataModel:

    def __init__(self, carol):
        self.carol = carol

        self.fields_dict = {}
        self.entity_template_ = {}

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order}
        else:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order,
                                 "sortBy": self.sort_by}

    def _get_name_type_data_models(self, fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_data_models(field['mdmFields'])
        return f

    def _get(self, id, by='id'):

        if by == 'name':
            url = f"v1/entities/templates/name/{id}"
        elif by == 'id':
            url = f"v1/entities/templates/{id}/working"
        else:
            raise print('Type incorrect, it should be "id" or "name"')

        #TODO: Add 'Not Found' and `is in Deleted state`
        resp = self.carol.call_api(url, method='GET')
        self.entity_template_ = {resp['mdmName']: resp}
        self.fields_dict.update({resp['mdmName']: self._get_name_type_data_models(resp['mdmFields'])})
        return resp


    def fetch_parquet(self, dm_name, merge_records=True, backend='dask', n_jobs=1, return_dask_graph=False, columns=None):
        """

        :param dm_name: `str`
            Data model name to be imported
        :param merge_records: `bool`, default `True`
            This will keep only the most recent record exported. Sometimes there are updates and/or deletions and
            one should keep only the last records.
        :param backend: ['dask','pandas'], default `dask`
            if to use either dask or pandas to fetch the data
        :param n_jobs: `int`, default `1`
            To be used with `backend='pandas'`. It is the number of threads to load the data from carol export.
        :param return_dask_graph: `bool`, default `false`
            If to return the dask graph or the dataframe.
        :param columns: `list`, default `None`
            List of columns to fetch.
        :return:
        """

        assert backend=='dask' or backend=='pandas'

        if return_dask_graph:
            assert backend == 'dask'

        #validate export
        dms = self._get_dm_export_stats()
        if not dms.get(dm_name):
            raise Exception(f'"{dm_name}" is not set to export data, \n use `dm = DataModel(login).export(dm_name="{dm_name}", sync_dm=True) to activate')

        carolina = Carolina(self.carol)
        carolina._init_if_needed()

        if backend=='dask':
            access_id = carolina.ai_access_key_id
            access_key = carolina.ai_secret_key
            aws_session_token = carolina.ai_access_token
            d =_import_dask(dm_name=dm_name, tenant_id=self.carol.tenant['mdmId'],
                            access_key=access_key, access_id=access_id, aws_session_token=aws_session_token,
                            merge_records=merge_records, golden=True, return_dask_graph=return_dask_graph, columns=columns)


        elif backend=='pandas':
            s3 = carolina.s3
            d = _import_pandas(s3=s3, dm_name=dm_name, tenant_id=self.carol.tenant['mdmId'],
                               n_jobs=n_jobs, golden=True, columns=columns)
            if d is None:
                warnings.warn("No data to fetch!", UserWarning)
                return None
        else:
            raise ValueError(f'backend should be "dask" or "pandas" you entered {backend}' )

        if merge_records:
            if not return_dask_graph:
                d.sort_values('mdmCounterForEntity', inplace=True)
                d.reset_index(inplace=True, drop=True)
                d.drop_duplicates(subset='mdmId', keep='last', inplace=True)
                d.reset_index(inplace=True, drop=True)
            else:
                d = d.set_index('mdmCounterForEntity', sorted=True) \
                    .drop_duplicates(subset='mdmId', keep='last') \
                    .reset_index(drop=True)

        return d


    def get_all(self, offset=0, page_size=-1, sort_order='ASC',
                sort_by=None, print_status=False,
                save_file=None):

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
        while count < self.total_hits:
            url_filter = "v1/entities/templates"
            query = self.carol.call_api(url_filter, params=self.query_params, method='GET')

            if query['count'] == 0:
                print('There are no more results.')
                print('Expecting {}, reponse = {}'.format(self.total_hits, count))
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
                                                      'mdmEntitySpace': i['mdmEntitySpace']}
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

        Export datamodel to s3

        This method will trigger or pause the export of the data in the datamodel to
        s3

        :param dm_name: `str`, default `None`
            Datamodel Name
        :param dm_id: `str`, default `None`
            Datamodel id
        :param sync_dm: `bool`, default `True`
            Sync the data model
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None
        """

        if sync_dm:
            status = 'RUNNING'
        else:
            status = 'PAUSED'

        if dm_name:
            dm_id = self.get_by_name(dm_name)['mdmId']
        else:
            assert dm_id

        query_params = {"status": status, "fullExport": full_export,
                        "deletePrevious":delete_previous}

        url = f'v1/entities/templates/{dm_id}/exporter'
        return self.carol.call_api(url, method='POST', params=query_params)

    def export_all(self, sync_dm=True, full_export=False, delete_previous=False):
        """

        Export all datamodel to s3

        This method will trigger or pause the export of the data in the datamodel to
        s3

        :param dm_name: `str`, default `None`
            Datamodel Name
        :param dm_id: `str`, default `None`
            Datamodel id
        :param sync_dm: `bool`, default `True`
            Sync the data model
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None
        """
        self.get_all()

        for i in self.template_dict.values():

            dm_id= i['mdmId']
            self.export(dm_id=dm_id, sync_dm=sync_dm, full_export=full_export,
                            delete_previous=delete_previous)

    def delete(self, dm_id=None, dm_name=None, entity_space='WORKING'):
        # TODO: Check Possible entity_spaces

        if dm_id is None:
            assert dm_name is not None
            resp = self.get_by_name(dm_name)
            dm_id = resp['mdmId']

        url = f"v1/entities/templates/{dm_id}"
        querystring = {"entitySpace": entity_space}

        return self.carol.call_api(url, method='DELETE', params=querystring)

    def _get_name_type_DMS(self, fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_DMS(field['mdmFields'])
        return f


    def _get_dm_export_stats(self):
        """
        Get export status for data models

        :return: `dict`
            dict with the information of which data model is exporting its data.
        """

        json_q = Filter.Builder(key_prefix="") \
            .must(TYPE_FILTER(value="mdmEntityTemplateExport")).build().to_json()

        query = Query(self.carol, index_type='CONFIG', page_size=1000, only_hits=False)
        query.query(json_q, ).go()

        dm_results = query.results
        dm_results = [elem.get('hits', elem) for elem in dm_results
                      if elem.get('hits', None)]
        dm_results = list(itertools.chain(*dm_results))

        dm = DataModel(self.carol).get_all().template_data
        dm = {i['mdmId']: i['mdmName'] for i in dm}

        if dm_results is not None:
            return {dm[i['mdmEntityTemplateId']]: i for i in dm_results}

        return dm_results

    def _get_min_max(self, query_filter=None):

        if query_filter is not None:
            j = query_filter.type(self.datamodel_name) \
                .aggregation_list([MINIMUM(name='MINIMUM',params= self.mdm_key), MAXIMUM(name='MAXIMUM',
                                                                                         params=self.mdm_key)])\
                .build().to_json()
        else:
            j = Filter.Builder()\
                .type(self.datamodel_name) \
                .aggregation_list([MINIMUM(name='MINIMUM',params= self.mdm_key), MAXIMUM(name='MAXIMUM',
                                                                                         params=self.mdm_key)])\
                .build().to_json()

        query = Query(self.carol, index_type=self.index_type, only_hits=False, get_aggs=True, save_results=False,
                      print_status=True, page_size=0).query(j).go()

        if query.results[0].get('aggs') is None:
            return None, None
        min_v = query.results[0]['aggs']['MINIMUM']['value']
        max_v = query.results[0]['aggs']['MAXIMUM']['value']
        print(f"Total Hits to reprocess: {query.total_hits}")
        return min_v, max_v

    def reprocess(self, datamodel_name, n_of_tasks=10, copy_or_move='move', record_type='ALL', query_filter=None):
        """
        Reprocess records from a data model

        :param datamodel_name: `str`
            Data model name
        :param n_of_tasks: `int`, default `10`
            Number of process to split the reprocess.
        :param copy_or_move: `str`, default `move`
            Either `move` or `copy` to staging.
        :param record_type:  `str`, default `ALL`
            Type of records to reprocess. `All`, `Golden` or `Rejected`
        :param query_filter:  `Filter.Builder Object`, default `None`
            The Filter instance to reprocess the data on.
        :return: None
        """

        assert copy_or_move=='copy' or copy_or_move=='move', 'copy_or_move shoulb be "copy" or "move"'
        if query_filter:
            assert isinstance(query_filter,Filter.Builder)

        if copy_or_move=='copy':
            copy_or_move = False
        else:
            copy_or_move = True

        self.mdm_id = self.get_by_name(datamodel_name)['mdmId']

        self.index_type = 'MASTER'
        self.datamodel_name = datamodel_name+'Golden'
        self.mdm_key = 'mdmCounterForEntity'
        min_v, max_v = self._get_min_max(query_filter=query_filter)

        chunks = ranges(min_v, max_v, n_of_tasks)
        print(f"Number of chunks: {len(chunks)}")


        url_filter = f"v1/entities/templates/{self.mdm_id}/reprocess"


        for c,i in enumerate(chunks):
            if query_filter is not None:
                query_filter_to_use = copy.deepcopy(query_filter)
                json_query = query_filter_to_use.must(RF(key=self.mdm_key, value=i)).build().to_json()
            else:
                json_query = Filter.Builder() \
                    .must(RF(key=self.mdm_key, value=i)) \
                    .build().to_json()

            query_params = {"recordType": record_type, "fuzzy": "false",
                           "deleteRecords": copy_or_move}

            result = self.carol.call_api(url_filter, data=json_query, params=query_params)
            print(f"To go: {c+1}/{len(chunks)}")

    #TODO: equal to Staging
    def send_a(self,session, url, data_json, extra_headers,content_type):
        self.carol.call_api(url, data=data_json, extra_headers=extra_headers,
                            content_type=content_type, session=session)


    #TODO: the same that in staging.
    async def _send_data_asynchronous(self, data, data_size, step_size, is_df, url, extra_headers,
                                      content_type, max_workers):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            session = self.carol._retry_session()
            # Set any session parameters here before calling `send_a`
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    self.send_a,
                    *(session, url, data_json, extra_headers, content_type)
                    # Allows us to pass in multiple arguments to `send_a`
                )
                for data_json in self._stream_data(data, data_size, step_size, is_df)
            ]

    def send_data(self, data, dm_name=None, dm_id=None, step_size=100, gzip=False,
                  print_stats=True, max_workers=None, async_send=False):

        """
        :param data: pandas data frame, json.
            Data to be send to Carol
        :param dm_name:  `str`, default `None`
            Data model name
        :param dm_id:  `str`, default `None`
            Data model id
        :param step_size: `int`, default `100`
            Number of records to be sent in each iteration. Max size for each batch is 10MB
        :param print_stats: `bool`, default `True`
            If print the status
        :param gzip: `bool`, default `True`
            If send each batch as a gzip file.
        :param max_workers: `int`, default `None`
            To be used with `async_send=True`. Number of threads to use when sending.
        :param async_send: `bool`, default `False`
            To use async to send the data. This is much faster than a sequential send.
        :return: None
        """

        self.gzip = gzip
        extra_headers = {}
        content_type = 'application/json'
        if self.gzip:
            content_type=None
            extra_headers["Content-Encoding"] = "gzip"
            extra_headers['content-type'] = 'application/json'

        if dm_name:
            dm_id = self.get_by_name(dm_name)['mdmId']
        else:
            assert dm_id

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



        url = f"v1/entities/templates/{dm_id}/goldenRecords?async=true"

        self.cont = 0
        if async_send:
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self._send_data_asynchronous(data, data_size, step_size, is_df,
                                                                        url, extra_headers, content_type, max_workers))
            loop.run_until_complete(future)

        else:
            for data_json in self._stream_data(data, data_size, step_size, is_df):
                self.carol.call_api(url, data=data_json, extra_headers=extra_headers, content_type=content_type)
                if print_stats:
                    print('{}/{} sent'.format(self.cont, data_size), end='\r')

    #TODO: reused from staging. Should I put in utils/?
    def _stream_data(self, data, data_size, step_size, is_df):
        for i in range(0, data_size, step_size):
            if is_df:
                data_to_send = data.iloc[i:i + step_size]
                self.cont += len(data_to_send)
                print('Sending {}/{}'.format(self.cont, data_size), end='\r')
                data_to_send = data_to_send.to_json(orient='records', date_format='iso', lines=False)
                if self.gzip:
                    out = io.BytesIO()
                    with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
                        f.write(data_to_send.encode('utf-8'))
                    yield out.getvalue()
                else:
                    yield json.loads(data_to_send)
            else:
                data_to_send = data[i:i + step_size]
                self.cont += len(data_to_send)
                print('Sending {}/{}'.format(self.cont, data_size), end='\r')
                if self.gzip:
                    out = io.BytesIO()
                    with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
                        f.write(json.dumps(data_to_send).encode('utf-8'))
                    yield out.getvalue()
                else:
                    yield data_to_send



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

        while True:
            url = 'v1/entities/templates/snapshot'
            resp = self.carol.call_api(url=url, method='POST', data=snapshot)

            if not resp.ok:
                if ('Record already exists' in self.lastResponse.json()['errorMessage']) and (overwrite):
                    del_DM = DataModel(self.carol)
                    del_DM.get_by_name(snapshot['entityTemplateName'])
                    dm_id = del_DM.entity_template_.get(snapshot['entityTemplateName']).get('mdmId', None)
                    if dm_id is None:  # if None
                        continue
                    entity_space = del_DM.entity_template_.get(snapshot['entityTemplateName'])['mdmEntitySpace']
                    del_DM.delete(dm_id=dm_id, entity_space=entity_space)
                    time.sleep(0.5)  # waint for deletion
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
        est_.get_by_name(self.dm_name)
        if est_.entity_template_ is not None:
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
        assert ((entity_template_type_ids is not None) or (entity_template_type_names is not None))

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
            resp = self.carol.call_api(url_filter, data=payload, method='POST', errors='ignore')
            # error handler for token
            if ('already exists' in resp.get('errorMessage',[])) and (overwrite):
                del_DM = DataModel(self.carol)
                del_DM.get_by_name(self.dm_name)
                dm_id = del_DM.entity_template_.get(self.dm_name).get('mdmId', None)
                if dm_id is None:  # if None
                    continue
                entity_space = del_DM.entity_template_.get(self.dm_name)['mdmEntitySpace']
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
            self.current_fields = [i for i in template_['mdmFieldsFull'].keys()]
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
                  label_map=None, description_map=None):

        if publish:
            assert profile_title in json_sample

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
                    if (ent_['mdmMappingDataType'].lower() == entity_type.ent_type):
                        self.add_field(prop, parent_field_id="")
                    else:
                        print('problem, {} not created, field name matches with an already'
                              'created field but different type'.format(prop))
                else:
                    print('Nested fields are not supported')
            else:
                if not entity_type.ent_type == 'nested':

                    current_label, current_description = self._labels_and_desc(prop)
                    self.fields.create(mdm_name=prop, mdm_mpping_data_type=entity_type.ent_type, mdm_field_type='PRIMITIVE',
                                       mdm_label=current_label, mdm_description=current_description)
                    self.all_possible_fields = self.fields.fields_dict
                    self.add_field(prop, parent_field_id="")
                else:
                    print('Nested fields are not supported')

        if publish:
            self._profile_title(profile_title, self.dm_id)
            self.publish_template(self.dm_id)


    # not done
    def _nested(self, mdmName, value, parentId=''):
        raise ValueError('not implemented')
        payload = {"mdmName": mdmName, "mdmMappingDataType": entity_type.ent_type,
                   "mdmLabel": {"en-US": mdmName}, "mdmDescription": {"en-US": mdmName}}
        entity_type = entType.get_ent_type_for(type(value))

        if entity_type == entObjectType and len(value) > 0:
            for key, val in value.items():
                payload['mdmFieldType'] = 'NESTED'
                print('criando NESTED')
                parentId = 1234
                create_field(key, val, parentId=parentId)

        elif entity_type == entArrayType and len(value) > 0:
            pass

        if entity_type.ent_type == 'nested':

            payload['mdmFieldType'] = 'NESTED'
            print('criando NESTED')
            parentId = 1234
            _, parentId = create_field()
        else:
            payload['mdmFieldType'] = 'PRIMITIVE'
            print('criando PRIMITIVE')

        return payload, parentId
