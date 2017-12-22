import requests
import json

class fieldsCarol(object):
    def __init__(self, token_object):
        self.dev = token_object.dev
        self.token_object = token_object
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.offset = 0
        self.pageSize = 100
        self.sortOrder = 'ASC'
        self.sortBy = None

        self.fields_dict = {}
        self.fields_data = []

        self.getAll(admin = True, print_status=False, save_file=False)

    def possibleTypes(self,admin=True):

        while True:
            if admin:
                url_filter = "https://{}.carol.ai{}/api/v1/fields/possibleTypes".format(self.token_object.domain, self.dev)
            else:
                url_filter = "https://{}.carol.ai{}/api/v1/admin/fields/possibleTypes".format(self.token_object.domain, self.dev)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            break
        self.lastResponse.encoding = 'utf8'
        self._possibleTypes = json.loads(self.lastResponse.text)


    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy}


    def getAll(self, admin = False, offset=0, pageSize=100, sortOrder='ASC', sortBy='mdmLastUpdated', print_status=True, save_file=False,
               filename='data/fields.json'):

        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.sortBy = sortBy
        self._setQuerystring()

        self.fields_dict = {}
        self.fields_data = []
        count = self.offset

        set_param = True
        self.totalHits = float("inf")
        if save_file:
            file = open(filename, 'w', encoding='utf8')

        if admin:
            url_filter = "https://{}.carol.ai{}/api/v1/admin/fields".format(self.token_object.domain, self.dev)
        else:
            url_filter = "https://{}.carol.ai{}/api/v1/fields".format(self.token_object.domain, self.dev)

        while count < self.totalHits:
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                if save_file:
                    file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)
            count += query['count']
            if set_param:
                self.totalHits = query["totalHits"]
                set_param = False

            query = query['hits']
            self.fields_data.extend(query)
            self.fields_dict.update({i['mdmName']: i for i in query})
            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()



    def getById(self, fields_ids=None, admin = False, save_file=False, filename='data/fields_ids.json'):

        assert fields_ids is not None

        self.fields_data = []

        if isinstance(fields_ids,str):
            vertical_ids = [fields_ids]

        if save_file:
            file = open(filename, 'w', encoding='utf8')

        for fields_id in fields_ids:
            if admin:
                url_filter = "https://{}.carol.ai{}/api/v1/admin/fields/{}".format(self.token_object.domain, self.dev,fields_id)
            else:
                url_filter = "https://{}.carol.ai{}/api/v1/fields/{}".format(self.token_object.domain, self.dev,fields_id)
            while True:
                self.lastResponse = requests.get(url=url_filter, headers=self.headers)
                if not self.lastResponse.ok:
                    # error handler for token
                    if self.lastResponse.reason == 'Unauthorized':
                        self.token_object.refreshToken()
                        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                        continue
                    if save_file:
                        file.close()
                    raise Exception(self.lastResponse.text)
                break

            self.lastResponse.encoding = 'utf8'
            query = [json.loads(self.lastResponse.text)]

            self.fields_data.extend(query)
            self.fields_dict.update({i['mdmName']: i for i in query})

            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()


    def create(self,mdmName,mdmMappingDataType,mdmFieldType,mdmLabel,mdmDescription):
        '''
        :param mdmName:
        :param mdmMappingDataType: string,  double, long, stc
        :param mdmFieldType: PRIMITIVE or NESTED
        :param mdmLabel:
        :param mdmDescription:
        :return:
        '''

        url = "https://{}.carol.ai{}/api/v1/fields".format(self.token_object.domain, self.dev)
        payload = {"mdmName": mdmName, "mdmMappingDataType": mdmMappingDataType, "mdmFieldType": mdmFieldType,
                   "mdmLabel": {"en-US": mdmLabel}, "mdmDescription": {"en-US": mdmDescription}}
        assert not mdmName in self.fields_dict.keys()

        while True:
            self.lastResponse = requests.request("POST", url, json=payload, headers=self.headers)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            break
        query = self.lastResponse.json()
        self.fields_dict.update({mdmName: query})




