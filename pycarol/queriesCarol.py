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
        self.scrollable = True
        self.get_all = True
        self.max_hits = float('inf')

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "indexType": self.indexType}
        else:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "indexType": self.indexType}

        if self.scrollable:
            self.querystring.update({"scrollable":self.scrollable})


    def _oldQueryHandler(self,type_query= 'query'):
        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')

        if type_query == 'query':
            url_filter = "https://{}.carol.ai/api/v2/queries/filter".format(self.token_object.domain)
        else:
            url_filter = "https://{}.carol.ai/api/v2/queries/named/{}".format(self.token_object.domain,
                                                                              self.named_query)

        while count < self.totalHits:
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, params=self.querystring,
                                              json=self.json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                if self.save_results:
                    file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)
            count += query['count']

            if set_param:
                if self.get_all:
                    self.totalHits = query["totalHits"]
                elif self.max_hits <= query["totalHits"]:
                    self.totalHits = self.max_hits
                else:
                    self.totalHits = query["totalHits"]

                set_param = False
                if self.safe_check:
                    self.mdmId_list = []
            if self.only_hits:
                query = query['hits']
                if self.safe_check:
                    self.mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                    if len(self.mdmId_list) > len(set(self.mdmId_list)):
                        raise Exception('There are repeated records')
                query = [elem['mdmGoldenFieldAndValues'] for elem in query]
                self.query_data.extend(query)
            else:
                query.pop('count')
                query.pop('took')
                query.pop('totalHits')
                self.query_data.append(query)

                if 'aggs' in query:
                    if self.save_results:
                        file.write(json.dumps(query, ensure_ascii=False))
                        file.write('\n')
                        file.flush()
                    break

            self.querystring['offset'] = count
            if self.print_status:
                print('{}/{}'.format(count, self.totalHits), end='\r')
            if self.save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if self.save_results:
            file.close()


    def _queryHandler_scroll(self,type_query='query'):
        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')


        if type_query == 'query':
            url_filter = "https://{}.carol.ai/api/v2/queries/filter".format(self.token_object.domain)
        else:
            url_filter = "https://{}.carol.ai/api/v2/queries/named/{}".format(self.token_object.domain, self.named_query)



        while count < self.totalHits:
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, params=self.querystring,
                                              json= self.json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                if self.save_results:
                    file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)

            count += query['count']
            scrollId = query['scrollId']
            url_filter = "https://{}.carol.ai/api/v2/queries/filter/{}".format(self.token_object.domain, scrollId)

            if set_param:

                if self.get_all:
                    self.totalHits = query["totalHits"]
                elif self.max_hits <= query["totalHits"]:
                    self.totalHits = self.max_hits
                else:
                    self.totalHits = query["totalHits"]

                self.querystring  = querystring = {"indexType":self.indexType}
                set_param = False
                if self.safe_check:
                    self.mdmId_list = []
            if self.only_hits:
                query = query['hits']
                if self.safe_check:
                    self.mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                    if len(self.mdmId_list) > len(set(self.mdmId_list)):
                        raise Exception('There are repeated records')
                query = [elem['mdmGoldenFieldAndValues'] for elem in query]
                self.query_data.extend(query)
            else:
                query.pop('count')
                query.pop('took')
                query.pop('totalHits')
                query.pop('scrollId')
                self.query_data.append(query)

                if 'aggs' in query:
                    if self.save_results:
                        file.write(json.dumps(query, ensure_ascii=False))
                        file.write('\n')
                        file.flush()
                    break

            if self.print_status:
                print('{}/{}'.format(count, self.totalHits), end='\r')
            if self.save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if self.save_results:
            file.close()

    def newQuery(self, json_query, use_scroll = True, max_hits = float('inf'), offset=0, pageSize=50, sortOrder='ASC', sortBy='mdmLastUpdated', indexType='MASTER',
                 only_hits = True, print_status=True, save_results=True, filename='query_result.json', safe_check=False):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.indexType = indexType
        self.sortBy = sortBy
        self.query_data = []
        self.only_hits = only_hits
        self.scrollable = use_scroll
        self.print_status = print_status
        self.save_results = save_results
        self.filename = filename
        self.safe_check = safe_check
        self.json_query = json_query
        self.max_hits = max_hits

        if max_hits == float('inf'):
            self.get_all = True
        else:
            self.get_all = False

        self._setQuerystring()

        if use_scroll:
            self._queryHandler_scroll(type_query='query')
        else:
            self._oldQueryHandler(type_query='query')



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

    def namedQuery(self, named_query, json_query, max_hits = float('inf'), use_scroll = True, offset=0, pageSize=50, sortOrder='ASC', indexType='MASTER',
                   only_hits=True, sortBy='mdmLastUpdated', safe_check= False,
                   print_status=True, save_results=True, filename='results_json.json'):

        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.indexType = indexType
        self.sortBy = sortBy
        self.query_data = []
        self.only_hits = only_hits
        self.named_query= named_query
        self.json_query = json_query
        self.safe_check =safe_check
        self.print_status = print_status
        self.save_results = save_results
        self.filename = filename
        self.use_scroll = use_scroll

        self.max_hits = max_hits

        if max_hits == float('inf'):
            self.get_all = True
        else:
            self.get_all = False

        self._setQuerystring()

        if use_scroll:
            self._queryHandler_scroll(type_query='named_query')
        else:
            self._oldQueryHandler(type_query='named_query')



class deleteFilter:
    def __init__(self, token_object):
        self.token_object = token_object
        self.indexType = 'MASTER'
        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.query_data = []
        self.querystring = {"indexType": self.indexType, "tenantId": self.token_object.tenantId}

    def deleteRegisters(self, json_query, indexType='MASTER',print_status=True):
        self.indexType = indexType
        self.query_data = []

        check_register = queryCarol(self.token_object).checkTotalHits(json_query)

        self.querystring = {"indexType": self.indexType, "tenantId": self.token_object.tenantId}

        _deleted = True
        self.totalHits = check_register
        while _deleted:
            url_filter = "https://{}.carol.ai/api/v2/queries/filter".format(self.token_object.domain)
            self.lastResponse = requests.delete(url=url_filter, headers=self.headers, params=self.querystring,
                                                json=json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            _deleted = False
        if print_status:
            print('{}/{} records deleted'.format(len(self.lastResponse.json()),check_register))
