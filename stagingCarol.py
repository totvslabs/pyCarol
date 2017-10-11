import json
import requests
from .schemaGenerator import *


class sendDataCarol:

    def __init__(self, token_object):

        self.token_object = token_object
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.stagingName = None
        self.step_size = 100
        self.url_filter = None
        self.print_stats = False

    def _streamData(self,data):
        i = 0
        size_data = len(data)
        while not i >= size_data:
            action = yield data[i:i + self.step_size]
            if action is None:
                i += self.step_size
        yield []

    def sendData(self, stagingName,data = None, step_size = 100, applicationId=None,print_stats = False):
        if applicationId is not None:
            if not applicationId==self.token_object.applicationId:
                self.token_object.newToken(applicationId)

        self.step_size = step_size
        self.print_stats = print_stats
        if not data:
            assert not self.data==[]
        else:
            self.data = data


        self.stagingName = stagingName
        self.url_filter = "https://{}.carol.ai/api/v2/staging/tables/{}?returnData=false&applicationId={}" \
            .format(self.token_object.domain, self.stagingName, self.token_object.applicationId)


        gen = self._streamData(self.data)

        data_size = len(self.data)
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
                    data_json = gen.send(True) #not needed?
                    print('Resending last batch, refreshing token')
                    continue
                raise Exception(response.reason)

            cont += len(data_json)
            if self.print_stats:
                print('{}/{} sent'.format(cont,data_size), end ='\r')
            data_json = gen.__next__()
            if data_json ==[]:
                ite = False

    @classmethod
    def from_json(cls, token, filename,read_lines = True):

        data = []
        with open(filename, 'r') as data_file:
            if read_lines:
                for file in data_file.readlines():
                    data.append(json.loads(file))
            else:
                json_data = data_file.read()
                data = json.loads(json_data)

        ret = sendDataCarol(token)
        ret.data = data
        return ret



class sendStagingTable(object):
    def __init__(self, token):

        self.token_object = token
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token,
                        'Content-Type': 'application/json'}

        self.applicationId =  self.token_object.applicationId
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



    def sendSchema(self,fields_dict=None,applicationId=None):
        if applicationId is not None:
            self.applicationId = applicationId
        if fields_dict is None:
            assert self.schema is not None
        elif isinstance(fields_dict,str):
            self.schema = json.loads(fields_dict)
        elif isinstance(fields_dict, dict):
            self.schema = fields_dict
        else:
            raise Exception('Not valid format')

        self.stagingName = self.schema['mdmStagingType']
        querystring = {"applicationId": self.applicationId}

        url = 'https://{}.carol.ai/api/v2/staging/tables/{}/schema'.format(self.token_object.domain,
                                                                                            self.stagingName)
        res = requests.request("POST", url, json=self.schema, headers=self.headers, params=querystring)


        if not res.ok:
            raise Exception(res.text)
        else:
            print('Schema sent succesfully!')



class getStaginCarol:
    def __init__(self, token_object):
        self.token_object = token_object
        if self.token_object.access_token is None:
            self.token_object.newToken()
        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        self.sortBy = None
        self.drop_list = None
        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.query_data = []
        self.table = None
        self.querystring = {}
        self.applicationId = self.token_object.applicationId
        self._setQuerystring()

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "applicationId": self.applicationId}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "applicationId": self.applicationId}

    def getStaging(self, table, applicationId=None, offset=0, pageSize=50, sortOrder='ASC', sortBy='mdmLastUpdated',
                   print_status=True, save_results=True, filename='staging_result.json', safe_check=False):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.table = table
        self.sortBy = sortBy
        self.query_data = []
        if applicationId is not None:
            self.applicationId = applicationId

        self._setQuerystring()

        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if save_results:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai/api/v2/staging/tables/{}".format(self.token_object.domain, self.table)
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

    def checkTotalHits(self, table, applicationId=None):
        self.table = table
        self.pageSize = 0
        if applicationId is not None:
            self.applicationId = applicationId

        self._setQuerystring()
        errors = True
        while errors:
            url_filter = "https://{}.carol.ai/api/v2/staging/tables/{}".format(self.token_object.domain, self.table)
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
