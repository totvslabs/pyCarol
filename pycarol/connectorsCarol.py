import json
import requests
#from . import utils

class connectorsCarol:
    def __init__(self, token_object):
        self.dev = token_object.dev
        self.token_object = token_object

        self.groupName = "Others"

        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}

        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        self.sortBy = None
        self.includeConnectors = False
        self.includeMappings = False
        self.includeConsumption = False

        self._setQuerystring()

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder, "includeMappings": self.includeMappings,
                                "includeConsumption": self.includeMappings, "includeConnectors": self.includeConnectors}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "includeMappings": self.includeMappings,
                                "includeConsumption": self.includeMappings, "includeConnectors": self.includeConnectors}


    def createConnector(self,connectorName, connectorLabel = None, groupName = "Others", overwrite=False):
        self.connectorName = connectorName
        if connectorLabel is None:
            self.connectorLabel = self.connectorName
        else:
            self.connectorLabel = connectorLabel

        self.groupName = groupName
        url = "https://{}.carol.ai{}/api/v1/connectors".format(self.token_object.domain, self.dev)


        payload = {  "mdmName": self.connectorName,   "mdmGroupName": self.groupName,   "mdmLabel": { "en-US": self.connectorLabel }}


        while True:
            self.response = requests.request("POST", url, json=payload,headers=self.headers)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue

                elif ('Record already exists' in self.response.json()['errorMessage']) and (overwrite):
                    self.getConnectorsByName(connectorName)
                    self.deleteConnector(self.connectorId)
                    continue
                raise Exception(self.response.text)
            break
        self.response = self.response.json()
        self.connectorId = self.response['mdmId']
        print('Connector created: connector ID = {}'.format(self.connectorId))


    def deleteConnector(self,connectorId, forceDeletion = True):
        self.connectorId = connectorId
        url = "https://{}.carol.ai{}/api/v1/connectors/{}".format(self.token_object.domain, self.dev,self.connectorId)
        querystring = {"forceDeletion": forceDeletion}

        while True:
            self.response = requests.request("DELETE", url, headers=self.headers, params=querystring)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.response.text)
            break
        print('Connector deleted')


    def getConnectorsByName(self,connectorName):
        self.connectorName = connectorName
        url = "https://{}.carol.ai{}/api/v1/connectors/name/{}".format(self.token_object.domain, self.dev,self.connectorName)

        while True:
            self.response = requests.request("GET", url, headers=self.headers)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.response.text)
            break
        self.response = self.response.json()
        self.connectorId = self.response['mdmId']
        self.connectorName = self.response['mdmName']
        print('Connector Id = {}'.format(self.connectorId))


    def getAll(self, offset=0, pageSize=-1, sortOrder='ASC', sortBy=None, includeConnectors = False, includeMappings = False,
               includeConsumption = False, print_status=True, save_results=False, filename='conectors.json'):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.sortBy = sortBy
        self.includeConnectors = includeConnectors
        self.includeMappings = includeMappings
        self.includeConsumption = includeConsumption

        self.connectors = []
        self._setQuerystring()

        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if save_results:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai{}/api/v1/connectors".format(self.token_object.domain, self.dev)
            self.lastResponse = requests.request("GET", url_filter, headers=self.headers, params=self.querystring)
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
            conn = json.loads(self.lastResponse.text)
            count += conn['count']
            if set_param:
                self.totalHits = conn["totalHits"]
                set_param = False
            conn = conn['hits']

            self.connectors.extend(conn)
            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_results:
                file.write(json.dumps(conn, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_results:
            file.close()

    def connectorStats(self,connectorId):


        url = "https://{}.carol.ai{}/api/v1/connectors/{}/stats".format(self.token_object.domain, self.dev,connectorId)
        while True:
            self.response = requests.request("GET", url, headers=self.headers)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.response.text)
            break
        conn_stats = self.response.json()['aggs']
        self.connectorsStats_ = {key : list(value['stagingEntityStats'].keys()) for key, value in conn_stats.items()}

