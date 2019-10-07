import json


class DataModelFields(object):
    def __init__(self, carol):

        self.carol = carol
        self.offset = 0
        self.page_size = 100
        self.sort_order = 'ASC'
        self.sort_by = None

        self.fields_dict = {}
        self.fields_data = []

        self.all(admin=True, print_status=False, save_file=False)

    def possible_types(self, admin=True):

        if not admin:
            url_filter = "v1/fields/possibleTypes"
        else:
            url_filter = "v1/admin/fields/possibleTypes"

        result = self.carol.call_api(url_filter)
        self._possible_types = result

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order}
        else:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order,
                                 "sortBy": self.sort_by}

    def all(self, admin=False, offset=0, page_size=100, sort_order='ASC', sort_by=None, print_status=True,
            save_file=False, filename='data/fields.json'):

        self.offset = offset
        self.pageSize = page_size
        self.sortOrder = sort_order
        self.sortBy = sort_by
        self._build_query_params()

        self.fields_dict = {}
        self.fields_data = []
        count = self.offset

        set_param = True
        self.total_hits = float("inf")
        if save_file:
            file = open(filename, 'w', encoding='utf8')

        if admin:
            url_filter = "v1/admin/fields"
        else:
            url_filter = "v1/fields"

        while count < self.total_hits:

            query = self.carol.call_api(url_filter, params=self.query_params)

            count += query['count']
            if set_param:
                self.total_hits = query["totalHits"]
                set_param = False

            query = query['hits']
            self.fields_data.extend(query)
            self.fields_dict.update({i['mdmName']: i for i in query})
            self.query_params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.total_hits), end='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()

    def get_by_id(self, fields_ids=None, admin=False, save_file=False,
                  filename='data/fields_ids.json'):

        assert fields_ids is not None

        self.fields_data = []

        if isinstance(fields_ids, str):
            fields_ids = [fields_ids]
        assert isinstance(fields_ids, list)

        if save_file:
            file = open(filename, 'w', encoding='utf8')

        for fields_id in fields_ids:
            if admin:
                url_filter = f"v1/admin/fields/{fields_id}"
            else:
                url_filter = f"v1/fields/{fields_id}"

            query = self.carol.call_api(url_filter)

            self.fields_data.extend(query)
            self.fields_dict.update({i['mdmName']: i for i in query})

            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()

    def create(self, mdm_name, mdm_mpping_data_type, mdm_field_type, mdm_label,
               mdm_description, admin=False):
        '''
        :param mdm_name:
        :param mdm_mpping_data_type: string,  double, long, stc
        :param mdm_field_type: PRIMITIVE or NESTED
        :param mdm_label:
        :param mdm_description:
        :param admin
        :return:
        '''

        if admin:
            url = "v1/admin/fields"
        else:
            url = "v1/fields"

        payload = {"mdmName": mdm_name, "mdmMappingDataType": mdm_mpping_data_type,
                   "mdmFieldType": mdm_field_type,
                   "mdmLabel": {"en-US": mdm_label}, "mdmDescription": {"en-US": mdm_description}}
        assert not mdm_name in self.fields_dict.keys()
        query = self.carol.call_api(method='POST', path=url, data=payload)

        self.fields_dict.update({mdm_name: query})
