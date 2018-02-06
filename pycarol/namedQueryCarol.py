import json
import requests
#from . import utils
import re

class namedQueries:
    def __init__(self, token_object):
        self.token_object = token_object
        self.dev = token_object.dev
        self.headers = self.token_object.headers_to_use
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
               filename='namedQueries.json', safe_check=False):
        '''
        Copy all named queries from a tenant
        '''

        self.named_query_data = []
        count = self.offset

        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        # self.indexType = indexType
        self.sortBy = sortBy
        set_param = True
        self._setQuerystring()
        self.totalHits = float("inf")
        if save_file:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai{}/api/v2/named_queries".format(self.token_object.domain, self.dev)
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
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        self._getParam()


    def getByName(self, named_query, save_file=False, filename='namedQueries.json'):
        '''
        Copy all named queries from a tenant
        '''

        self.named_query_data = []

        if save_file:
            file = open(filename, 'w', encoding='utf8')
        while True:
            url_filter = "https://{}.carol.ai{}/api/v2/named_queries/name/{}".format(self.token_object.domain, self.dev,named_query)
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
        query = json.loads(self.lastResponse.text)

        self.named_query_dict.update({query['mdmQueryName']: query})

        if save_file:
            file.write(json.dumps(query, ensure_ascii=False))
            file.write('\n')
            file.flush()
            file.close()
        self._getParam()

    def getParamByName(self, named_query=None):
        self.getByName(named_query, save_file=False)
        return self.paramDict

    def creatingNamedQueries(self, namedQueries,overwrite = True):
        '''
        Create named queries at the new tenant.
        :param token: AccessToken tenant
        :param tenant: tenant domain
        :param namedQueries: list of named queries to be sent
        :return: empty
        '''


        url_filter = 'https://{}.carol.ai{}/api/v2/named_queries'.format(self.token_object.domain, self.dev)
        count = 0
        rType = "POST"
        for query in namedQueries:
            count+=1
            query.pop('mdmId', None)
            query.pop('mdmTenantId', None)
            while True:
                self.lastResponse  = requests.request(rType ,url=url_filter, headers=self.headers, json=query)
                if not self.lastResponse.ok:
                    # error handler for token
                    if self.lastResponse.reason == 'Unauthorized':
                        self.token_object.refreshToken()
                        self.headers = {'Authorization': self.token_object.access_token,
                                        'Content-Type': 'application/json'}
                        continue
                    elif ('Record already exists' in self.lastResponse.json()['errorMessage']) and (overwrite):
                        self.getByName(query['mdmQueryName'])
                        mdmId = self.named_query_dict[query['mdmQueryName']]['mdmId']
                        url_filter = 'https://{}.carol.ai{}/api/v2/named_queries/{}'.format(self.token_object.domain,
                                                                                         self.dev,mdmId)


                        rType = "PUT"
                        continue

                    raise Exception(self.lastResponse.text)
                break
            url_filter = 'https://{}.carol.ai{}/api/v2/named_queries'.format(self.token_object.domain, self.dev)
            rType = "POST"
            print('{}/{} named queries copied'.format(count, len(namedQueries)), end='\r')


    def deleteNamedQueries(self,namedQueries):
        '''
        Delete named query from a tenant
        :param namedQueries: list of named queries to be deleted
        :return:
        '''
        if isinstance(namedQueries,str):
            namedQueries = [namedQueries]

        for name in namedQueries:
            url_filter = 'https://{}.carol.ai{}/api/v1/namedQueries/name/{}'.format(self.token_object.domain, self.dev, name)
            response = requests.delete(url=url_filter, headers=self.headers)
            print('Deleting named query: {}'.format(name))
        print('Finished!')
