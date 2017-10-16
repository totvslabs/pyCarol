import json
import requests
#from . import utils
import re


class queryCarol:
    def __init__(self, token_object):
        self.token_object = token_object
        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        self.sortBy = None
        self.indexType = 'MASTER'
        self.drop_list = None
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.query_data = []
        self.querystring = {}
        self.only_hits = True

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "indexType": self.indexType}
        else:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "indexType": self.indexType}


    def _setParams(self):
        pass

    def _dropHandler(self):
        pass

    def newQuery(self, json_query, offset=0, pageSize=50, sortOrder='ASC', sortBy='mdmLastUpdated', indexType='MASTER',
                 only_hits = True, print_status=True, save_results=True, filename='query_result.json', safe_check=False):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.indexType = indexType
        self.sortBy = sortBy
        self.query_data = []
        self.only_hits = only_hits

        self._setQuerystring()

        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if save_results:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai/api/v2/queries/filter".format(self.token_object.domain)
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, params=self.querystring,
                                              json=json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                if save_results:
                    file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)
            count += query['count']

            if set_param:
                self.totalHits = query["totalHits"]
                # total_pages = self.totalHits // pageSize + 1
                set_param = False
                if safe_check:
                    mdmId_list = []
            if self.only_hits:
                query = query['hits']
                if safe_check:
                    mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                    if len(mdmId_list) > len(set(mdmId_list)):
                        raise Exception('There are repeated records')
                query = [elem['mdmGoldenFieldAndValues'] for elem in query]
                self.query_data.extend(query)
            else:
                query.pop('count')
                query.pop('took')
                query.pop('totalHits')
                self.query_data.append(query)

                if 'aggs' in query:
                    if save_results:
                        file.write(json.dumps(query, ensure_ascii=False))
                        file.write('\n')
                        file.flush()
                    break

            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_results:
            file.close()

    def checkTotalHits(self, json_query):
        errors = True
        while errors:
            url_filter = "https://{}.carol.ai/api/v2/queries/filter?offset={}&pageSize={}&sortOrder={}&indexType={}".format(
                self.token_object.domain, str(self.offset), str(0), self.sortOrder, self.indexType)
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, json=json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            errors = False

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        self.totalHits = query["totalHits"]
        return self.totalHits

    def namedQuery(self, named_query, json_query, offset=0, pageSize=50, sortOrder='ASC', indexType='MASTER',
                   only_hits=True, sortBy='mdmLastUpdated', safe_check='False',
                   print_status=True, save_result=True, filename='results_json.json'):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.indexType = indexType
        self.sortBy = sortBy
        self.query_data = []
        self.only_hits = only_hits

        self._setQuerystring()

        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if save_result:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai/api/v2/queries/named/{}".format(self.token_object.domain, named_query)
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, params=self.querystring,
                                              json=json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                if save_result:
                    file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)
            count += query['count']
            if set_param:
                self.totalHits = query["totalHits"]
                set_param = False
                if safe_check:
                    mdmId_list = []


            if self.only_hits:
                query = query['hits']
                if safe_check:
                    mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                    if len(mdmId_list) > len(set(mdmId_list)):
                        raise Exception('There are repeated records')
                query = [elem['mdmGoldenFieldAndValues'] for elem in query]
                self.query_data.extend(query)
            else:
                query.pop('count')
                query.pop('took')
                query.pop('totalHits')
                self.query_data.append(query)
                if save_result:
                    file.write(json.dumps(query, ensure_ascii=False))
                    file.write('\n')
                    file.flush()
                    break

            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_result:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_result:
            file.close()


class namedQueryManagement:
    def __init__(self, token_object):
        self.token_object = token_object
        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        # self.indexType = indexType
        self.sortBy = None
        self.named_query_data = []
        self.named_query_dict = {}

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy}

    def _getParam(self, named_query=None):
        assert self.named_query_dict
        self.paramDict = {}
        if named_query is None:
            for key, value in self.named_query_dict.items():
                self.paramDict[key] = re.findall(r'\{\{(.*?)\}\}', json.dumps(value, ensure_ascii=False))

    def getAll(self, offset=0, pageSize=50, sortOrder='ASC', sortBy='mdmLastUpdated', print_status=True, save_file=True,
               filename='data/namedQueries.json', safe_check=False):
        '''
        Copy all named queries from a tenant
        '''
        self._setQuerystring()
        self.named_query_data = []
        count = self.offset

        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        # self.indexType = indexType
        self.sortBy = sortBy
        set_param = True
        self.totalHits = float("inf")
        file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai/api/v2/named_queries".format(self.token_object.domain)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)
            count += query['count']
            if set_param:
                self.totalHits = query["totalHits"]
                # total_pages = self.totalHits // pageSize + 1
                set_param = False
                if safe_check:
                    mdmId_list = []
            query = query['hits']
            if safe_check:
                mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                if len(mdmId_list) > len(set(mdmId_list)):
                    raise Exception('There are repeated records')

            self.named_query_data.extend(query)
            self.named_query_dict.update({i['mdmQueryName']: i for i in query})
            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            file.write(json.dumps(query, ensure_ascii=False))
            file.write('\n')
            file.flush()
        file.close()
        self._getParam()

    def creatingNamedQueries(self, namedQueries):
        '''
        Create named queries at the new tenant.
        :param token: AccessToken tenant
        :param tenant: tenant domain
        :param namedQueries: list of named queries to be sent
        :return: empty
        '''

        errors = True
        while errors:
            url_filter = "https://{}.carol.ai/api/v2/queries/filter?offset={}&pageSize={}&sortOrder={}&indexType={}".format(
                self.token_object.domain, str(self.offset), str(0), self.sortOrder, self.indexType)
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, json=namedQueries)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            errors = False

        url_filter = 'https://{}.carol.ai/api/v1/namedQueries'.format(self.token_object.domain)
        for query in namedQueries:
            query.pop('mdmId', None)
            query.pop('mdmTenantId', None)
            response = requests.post(url=url_filter, headers=self.headers, json=query)
            if not response.ok:
                print('Error sending named query: {}'.format(response.text))
        print('Finished!')

    def deleteNamedQueries(accessToken, tenant, namedQueries):
        '''
        Delete named query from a tenant
        :param accessToken: AccessToken
        :param tenant: tenant domain
        :param namedQueries: list of named queries to be deleted
        :return:
        '''
        headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
        names = [name['mdmQueryName'] for name in namedQueries]
        for name in names:
            url_filter = 'https://{}.carol.ai/api/v1/namedQueries/name/{}'.format(tenant, name)
            response = requests.delete(url=url_filter, headers=headers)
            print('Deleting named query: {}'.format(name))
        print('Finished!')
