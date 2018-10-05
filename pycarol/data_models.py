import json


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

    def _get_name_type_data_models(self,fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_data_models(field['mdmFields'])
        return f

    def _get(self,id, by = 'id'):

        if by == 'name':
            url = f"v1/entities/templates/name/{id}"
        elif by == 'id':
            url = f"v1/entities/templates/{id}/working"
        else:
            raise print('Type incorrect, it should be "id" or "name"')

        resp = self.carol.call_api(url, method='GET')
        self.entity_template_ = {resp['mdmName'] : resp}
        self.fields_dict.update({resp['mdmName']: self._get_name_type_data_models(resp['mdmFields'])})
        return resp


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
            assert isinstance(save_file,str)
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
                                                      'mdmEntitySpace': i['mdmEntitySpace'] }
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

    def get_by_name(self,name):

        self._get(name, by = 'name')
        return self

    def get_by_id(self,id):
        self._get(id, by='id')
        return self

    def get_snapshot(self,dm_id,entity_space):

        url_snapshot = f'v1/entities/templates/{dm_id}/snapshot?entitySpace={entity_space}'
        resp = self.carol.call_api(url_snapshot, method='GET')
        self.snapshot_ = {resp['entityTemplateName']: resp}
        return self


    def export(self, dm_name=None, dm_id=None, status=True):

        if dm_name:
            dm_id= self.get_by_name(dm_name).entity_template_.get(dm_name)['mdmId']

        else:
            assert dm_id

        url = f'v1/entities/templates/{dm_id}/exporter/{status}'
        self.carol.call_api(url, method='POST')

