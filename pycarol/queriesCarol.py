import json
import requests
from . import namedQueryCarol as nq
#from . import utils
import re


class queryCarol:
    """ It implements the calls for the following endpoints:
       1. /api/v2/queries/filter
       2. /api/v2/queries/filtera
       3. /api/v2/queries/named/{query_name}

    :param token_object: An object of the class loginCarol that contains the tenant information needed to generate access tokens.
    See loginCarol docstring
    Usage::
      >>> from pycarol.queriesCarol import  queryCarol
      >>> query = queryCarol(token_object)
    """
    def __init__(self, token_object):
        self.dev = token_object.dev
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
        self.get_errors = False
        self.safe_check = False
        self.fields = None

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "indexType": self.indexType}
        else:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "indexType": self.indexType}

        if self.scrollable:
            self.querystring.update({"scrollable":self.scrollable})
        if self.fields:
            self.querystring.update({"fields": self.fields})

    @staticmethod
    def _setReturnFields(fields):
        fields = ','.join(fields)
        return fields


    def _oldQueryHandler(self,type_query= 'query'):
        set_param = True

        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')

        if type_query == 'query':
            url_filter = "https://{}.carol.ai{}/api/v2/queries/filter".format(self.token_object.domain, self.dev)
        else:
            url_filter = "https://{}.carol.ai{}/api/v2/queries/named/{}".format(self.token_object.domain, self.dev,
                                                                              self.named_query)
        count = self.offset
        toGet = float("inf")
        downloaded = 0
        while count < toGet:
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
            downloaded += query['count']
            if set_param:
                self.totalHits = query["totalHits"]
                if self.get_all:
                    toGet = query["totalHits"]
                elif self.max_hits <= query["totalHits"]:
                    toGet = self.max_hits
                else:
                    toGet = query["totalHits"]

                set_param = False
                if self.safe_check:
                    self.mdmId_list = []
                if self.get_errors:
                    self.query_errors = {}

            if self.only_hits:
                query = query['hits']
                if self.safe_check:
                    self.mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                    if len(self.mdmId_list) > len(set(self.mdmId_list)):
                        raise Exception('There are repeated records')

                if self.get_errors:
                    self.query_errors.update({elem.get('mdmId',elem) :  elem.get('mdmErrors',elem) for elem in query if elem['mdmErrors']})

                query = [elem.get('mdmGoldenFieldAndValues',elem) for elem in query if elem.get('mdmGoldenFieldAndValues',elem)]  #get mdmGoldenFieldAndValues if not empty and if it exists
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
                print('{}/{}'.format(downloaded, toGet), end='\r')
            if self.save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if self.save_results:
            file.close()


    def _queryHandler_scroll(self,type_query='query'):

        if not self.offset == 0:
            #self.offset = 0
            raise ValueError('It is not possible to use offset when use scroll for pagination')

        set_param = True
        count = self.offset
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')

        if type_query == 'query':
            url_filter = "https://{}.carol.ai{}/api/v2/queries/filter".format(self.token_object.domain, self.dev)
        else:
            url_filter = "https://{}.carol.ai{}/api/v2/queries/named/{}".format(self.token_object.domain, self.dev, self.named_query)

        toGet = float("inf")
        downloaded = 0
        while count < toGet:
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
            downloaded += query['count']
            scrollId = query.get('scrollId', None)
            url_filter = "https://{}.carol.ai{}/api/v2/queries/filter/{}".format(self.token_object.domain, self.dev, scrollId)

            if set_param:
                self.totalHits = query["totalHits"]
                if self.get_all:
                    toGet = query["totalHits"]
                elif self.max_hits <= query["totalHits"]:
                    toGet = self.max_hits
                else:
                    toGet = query["totalHits"]

                #self.querystring = {"indexType":self.indexType}
                set_param = False
                if self.safe_check:
                    self.mdmId_list = []
                if self.get_errors:
                    self.query_errors = {}

            if self.only_hits:
                query = query['hits']
                if self.safe_check:
                    self.mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                    if len(self.mdmId_list) > len(set(self.mdmId_list)):
                        raise Exception('There are repeated records')

                if self.get_errors:
                    self.query_errors.update({elem.get('mdmId',elem) :  elem.get('mdmErrors',elem) for elem in query if elem['mdmErrors']})

                query = [elem.get('mdmGoldenFieldAndValues',elem) for elem in query if elem.get('mdmGoldenFieldAndValues',elem)]  #get mdmGoldenFieldAndValues if not empty and if it exists
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
                print('{}/{}'.format(downloaded, toGet), end='\r')
            if self.save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if self.save_results:
            file.close()

    def newQuery(self, json_query, max_hits = float('inf'), offset=0, pageSize=50, sortOrder='ASC', use_scroll = True,
                 sortBy='mdmLastUpdated', indexType='MASTER',only_hits = True, print_status=True, fields =None,
                 save_results=True, filename='query_result.json', safe_check=False, get_errors = False):

        """

        :param json_query: Json object with the query to use
        :param use_scroll: Use scroll endpoint
        :param max_hits: number of records to return
        :param offset: page offset
        :param pageSize: Number of register to return per call
        :param sortOrder: Sort order
        :param sortBy: Filter to sort response
        :param indexType: Type of index, MASTER, REJECTED, etc
        :param only_hits:  Return only values inside the path $.hits.
        :param print_status: Print how many records were downloaded.
        :param save_results: Save or not the result
        :param filename: File pacth and name to save the response
        :param safe_check: Check for repeated records
        :return: The response will be in the variable "query_data"
        """

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
        self.get_errors = get_errors
        self.json_query = json_query
        self.max_hits = max_hits

        if isinstance(fields,str):
            fields = [fields]
            fields = self._setReturnFields(fields)
        elif fields is not None:
            fields = self._setReturnFields(fields)

        self.fields = fields

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
        """
        Check the total hits for a given query
        :param json_query: Json object with the query to use
        :return: number of records for this query
        """
        errors = True
        while errors:
            url_filter = "https://{}.carol.ai{}/api/v2/queries/filter?offset={}&pageSize={}&sortOrder={}&indexType={}".format(
                self.token_object.domain, self.dev, str(self.offset), str(0), self.sortOrder, self.indexType)
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

    def namedQuery(self, named_query, json_query, max_hits = float('inf'), use_scroll = True, offset=0, pageSize=50,
                   sortOrder='ASC', indexType='MASTER', fields =None,
                   only_hits=True, sortBy='mdmLastUpdated', safe_check= False,
                   print_status=True, save_results=False, filename='results_json.json'):

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

        if isinstance(fields,str):
            fields = [fields]
            fields = self._setReturnFields(fields)
        elif fields is not None:
            fields = self._setReturnFields(fields)
        self.fields = fields

        if max_hits == float('inf'):
            self.get_all = True
        else:
            self.get_all = False

        self._setQuerystring()

        if use_scroll:
            self._queryHandler_scroll(type_query='named_query')
        else:
            self._oldQueryHandler(type_query='named_query')

    def namedQueryParams(self,named_query):
        named = nq.namedQueries(self.token_object)
        named.getParamByName(named_query=named_query)
        return named.paramDict


    def downloadAll(self, dm_name, connectorId = None, pageSize=500, save_results = False,safe_check = False, use_scroll = True,
                    filename ='allResults.json',print_status=True, max_hits = float('inf'), from_stag = False, get_errors = False,
                    only_hits=True, fields =None):
        if from_stag:
            assert connectorId is not None
            indexType = 'STAGING'
            json_query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": connectorId+'_'+dm_name}]}
        else:
            json_query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": dm_name}]}
            indexType = 'MASTER'


        self.newQuery(json_query=json_query, pageSize=pageSize, save_results=save_results, only_hits= only_hits, indexType= indexType,
                      safe_check=safe_check,filename=filename, print_status=print_status, max_hits = max_hits, use_scroll = use_scroll,
                      get_errors=get_errors, fields = fields)



class deleteFilter:
    def __init__(self, token_object):
        self.dev = token_object.dev
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
            url_filter = "https://{}.carol.ai{}/api/v2/queries/filter".format(self.token_object.domain, self.dev)
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



    def deleteAll(self, dm_name, indexType='MASTER',print_status=True):
        json_query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": dm_name}]}
        self.deleteRegisters(json_query, indexType=indexType,print_status=print_status)


