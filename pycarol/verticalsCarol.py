import requests
import json


class verticals(object):
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

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy}

    def getAll(self, offset=0, pageSize=100, sortOrder='ASC', sortBy='mdmLastUpdated', print_status=True, save_file=False,
               filename='data/verticalsIds.json'):

        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.sortBy = sortBy
        self._setQuerystring()

        self.verticals_dict = {}
        self.verticals_data = []
        count = self.offset


        set_param = True
        self.totalHits = float("inf")
        if save_file:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai{}/api/v1/verticals".format(self.token_object.domain, self.dev)
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
            self.verticals_data.extend(query)
            self.verticals_dict.update({i['mdmName']: i['mdmId'] for i in query})
            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self.verticals_dict

    def getById(self, vertical_ids=None, save_file=False, filename='data/verticalsId.json'):

        assert vertical_ids is not None

        if isinstance(vertical_ids,str):
            vertical_ids = [vertical_ids]

        if save_file:
            file = open(filename, 'w', encoding='utf8')

        self.verticals_dict = {}
        self.verticals_data = []
        for vertical_id in vertical_ids:
            while True:
                url_filter = "https://{}.carol.ai{}/api/v1/verticals/{}".format(self.token_object.domain, self.dev,vertical_id)
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

            self.verticals_data.extend(query)
            self.verticals_dict.update({i['mdmName']: i['mdmId'] for i in query})

            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self.verticals_dict

    def getByName(self, names=None, save_file=False, filename='data/verticalsId.json'):

        assert names is not None

        if isinstance(names, str):
            names = [names]

        if save_file:
            file = open(filename, 'w', encoding='utf8')

        self.verticals_dict = {}
        self.verticals_data = []
        for name in names:
            while True:
                url_filter = "https://{}.carol.ai{}/api/v1/verticals/name/{}".format(self.token_object.domain, self.dev, name)
                self.lastResponse = requests.get(url=url_filter, headers=self.headers)
                if not self.lastResponse.ok:
                    # error handler for token
                    if self.lastResponse.reason == 'Unauthorized':
                        self.token_object.refreshToken()
                        self.headers = {'Authorization': self.token_object.access_token,
                                        'Content-Type': 'application/json'}
                        continue
                    if save_file:
                        file.close()
                    raise Exception(self.lastResponse.text)
                break

            self.lastResponse.encoding = 'utf8'
            query = [json.loads(self.lastResponse.text)]

            self.verticals_data.extend(query)
            self.verticals_dict.update({i['mdmName']: i['mdmId'] for i in query})

            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self.verticals_dict
