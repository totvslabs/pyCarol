import json
import requests
from .schemaGenerator import *
import pandas as pd


class sendDataCarol:

    def __init__(self, token_object):
        self.dev = token_object.dev
        self.token_object = token_object
        self.headers = self.token_object.headers_to_use
        self.stagingName = None
        self.step_size = 500
        self.url_filter = None
        self.print_stats = False
        self.read_lines = False
        self.isDF = False
        self.data_size = None
        self.data = None

    def _streamData(self):

        for i in range(0, self.data_size, self.step_size):
            if self.isDF:
                data = self.data.iloc[i:i + self.step_size].to_json(orient='records',
                                                                    date_format='iso',
                                                                    lines=False)
                data = json.loads(data)
                yield data
            else:
                yield self.data[i:i + self.step_size]

        yield []

    def sendData(self, stagingName, data = None, step_size = 100,
                 connectorId=None, print_stats = False):
        if connectorId is not None:
            if not connectorId==self.token_object.connectorId:
                self.token_object.newToken(connectorId)

        self.step_size = step_size
        self.print_stats = print_stats
        if data is None:
            assert not self.data == []
            self.data_size = len(self.data)
        else:
            if isinstance(data,pd.DataFrame):
                self.isDF = True
                self.data = data
                self.data_size = self.data.shape[0]

            elif isinstance(data,str):
                self.data = json.loads(data)
                self.data_size = len(self.data)
            else:
                self.data = data
                self.data_size = len(self.data)

        if  (not isinstance(self.data, list)) and (not self.isDF) :
            self.data = [self.data]
            self.data_size = len(self.data)


        self.stagingName = stagingName
        self.url_filter = "https://{}.carol.ai{}/api/v2/staging/tables/{}?returnData=false&connectorId={}" \
            .format(self.token_object.domain, self.dev,
                    self.stagingName, self.token_object.connectorId)

        gen = self._streamData()
        cont = 0
        ite = True
        data_json = gen.__next__()
        while ite:
            response = requests.post(url=self.url_filter, headers=self.headers, json=data_json)
            if not response.ok:
                # error handler for token
                if response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    print('Resending last batch, refreshing token')
                    continue
                raise Exception(response.text)

            cont += len(data_json)
            if self.print_stats:
                print('{}/{} sent'.format(cont,self.data_size) , end ='\r')
            data_json = gen.__next__()
            if data_json ==[]:
                break

    @classmethod
    def from_json(cls, token, filename, read_lines = True):

        with open(filename, 'r') as data_file:
            if read_lines:
                data = []
                for file in data_file.readlines():
                    data.append(json.loads(file))
            else:
                data = json.load(data_file)

        ret = sendDataCarol(token)
        ret.data = data
        ret.read_lines = read_lines
        return ret



class stagingSchema(object):
    def __init__(self, token):
        self.dev = token.dev
        self.token_object = token
        self.headers = self.token_object.headers_to_use

        self.connectorId =  self.token_object.connectorId
        self.schema = None

    def createSchema(self,fields_dict=None,mdmStagingType='stagingName', mdmFlexible='false',
                     crosswalkname=None,crosswalkList=None):

        assert fields_dict is not None

        if isinstance(fields_dict,dict):
            self.schema = carolSchemaGenerator(fields_dict)
            self.schema =  self.schema.to_dict(mdmStagingType=mdmStagingType, mdmFlexible=mdmFlexible,
                                               crosswalkname=crosswalkname,crosswalkList=crosswalkList)
        elif isinstance(fields_dict,str):

            self.schema = carolSchemaGenerator.from_json(fields_dict)
            self.schema = self.schema.to_dict(mdmStagingType=mdmStagingType, mdmFlexible=mdmFlexible,
                                              crosswalkname=crosswalkname, crosswalkList=crosswalkList)


    def sendSchema(self, fields_dict=None, connectorId=None, request_type = 'POST',overwrite = False):
        if connectorId is not None:
            self.connectorId = connectorId
            self.token_object.newToken(self.connectorId)
        if fields_dict is None:
            assert self.schema is not None
        elif isinstance(fields_dict,str):
            self.schema = json.loads(fields_dict)
        elif isinstance(fields_dict, dict):
            self.schema = fields_dict
        else:
            raise Exception('Not valid format')

        self.stagingName = self.schema['mdmStagingType']
        querystring = {"connectorId": self.connectorId}

        url = 'https://{}.carol.ai{}/api/v2/staging/tables/{}/schema'.format(self.token_object.domain, self.dev,
                                                                           self.stagingName)
        while True:
            self.response = requests.request(request_type, url, json=self.schema, headers=self.headers, params=querystring)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue

                elif ('Record already exists' in self.response.json()['errorMessage']) and (overwrite):
                    request_type = 'PUT'
                    continue
                raise Exception(self.response.text)
            break
        print('Schema sent succesfully!')
        self.response = self.response.json()


    def getSchema(self,stagingName,connectorId):

        self.schema = {}

        querystring = {"connectorId": connectorId}
        while True:
            url = "https://{}.carol.ai{}/api/v2/staging/tables/{}/schema".format(self.token_object.domain, self.dev,stagingName)
            self.response = requests.request("GET", url, headers=self.headers, params=querystring)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.response.text)
            break

        self.schema = self.response.json()



class getStagingDataCarol:
    def __init__(self, token_object):
        self.dev = token_object.dev
        self.token_object = token_object
        self.headers = self.token_object.headers_to_use
        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        self.sortBy = None
        self.drop_list = None
        self.query_data = []
        self.table = None
        self.querystring = {}
        self.connectorId = self.token_object.connectorId
        self._setQuerystring()

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "connectorId": self.connectorId}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "connectorId": self.connectorId}

    def getData(self, table, connectorId=None, offset=0, pageSize=50, sortOrder='ASC', sortBy='mdmLastUpdated',
                   print_status=True, save_results=True, filename='staging_result.json', safe_check=False):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.table = table
        self.sortBy = sortBy
        self.query_data = []
        if connectorId is not None:
            self.connectorId = connectorId

        self._setQuerystring()

        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if save_results:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai{}/api/v2/staging/tables/{}".format(self.token_object.domain, self.dev, self.table)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
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
                set_param = False
                if safe_check:
                    mdmId_list = []
            query = query['hits']
            if safe_check:
                mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                if len(mdmId_list) > len(set(mdmId_list)):
                    raise Exception('There are repeated records')

            self.query_data.extend(query)
            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_results:
            file.close()

    def checkTotalHits(self, table, connectorId=None):
        self.table = table
        self.pageSize = 0
        if connectorId is not None:
            self.connectorId = connectorId

        self._setQuerystring()
        errors = True
        while errors:
            url_filter = "https://{}.carol.ai{}/api/v2/staging/tables/{}".format(self.token_object.domain, self.dev, self.table)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
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