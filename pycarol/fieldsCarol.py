import requests
import json

class fields(object):
    def __init__(self, token_object):
        self.token_object = token_object
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.offset = 0
        self.pageSize = 100
        self.sortOrder = 'ASC'
        self.sortBy = None
        self.possibleTypes

    def possibleTypes(self,admin=True):

        while True:
            if admin:
                url_filter = "https://{}.carol.ai/api/v1/fields/possibleTypes".format(self.token_object.domain)
            else:
                url_filter = "https://{}.carol.ai/api/v1/admin/fields/possibleTypes".format(self.token_object.domain)
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
            url_filter = "https://{}.carol.ai/api/v1/admin/fields".format(self.token_object.domain)
        else:
            url_filter = "https://{}.carol.ai/api/v1/fields".format(self.token_object.domain)

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
        return self.fields_dict



    def getById(self, fields_ids=None, admin = False, save_file=False, filename='data/fields_ids.json'):

        assert fields_ids is not None

        if isinstance(fields_ids,str):
            vertical_ids = [fields_ids]

        if save_file:
            file = open(filename, 'w', encoding='utf8')

        self.fields_dict = {}
        self.fields_data = []

        for fields_id in fields_ids:
            if admin:
                url_filter = "https://{}.carol.ai/api/v1/admin/fields/{}".format(self.token_object.domain,fields_id)
            else:
                url_filter = "https://{}.carol.ai/api/v1/fields/{}".format(self.token_object.domain,fields_id)
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
        return self.fields_dict


