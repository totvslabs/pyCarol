import json
import requests

class tenantApps:

    def __init__(self, token_object):
        self.dev = token_object.dev
        self.token_object = token_object

        self.headers = self.token_object.headers_to_use
        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        self.entitySpace = 'PRODUCTION'
        self.sortBy = None


    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "entitySpace": self.entitySpace}
        else:
            self.querystring = {"offset": self.offset, "pageSize": self.pageSize, "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "entitySpace": self.entitySpace}

    def getAll(self, entitySpace = 'PRODUCTION', pageSize = 50 ,offset = 0, sortOrder = 'ASC' , sortBy = None):


        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.entitySpace = entitySpace
        self.sortBy = sortBy
        self._setQuerystring()

        while True:
            url_filter = "https://{}.carol.ai{}/api/v1/tenantApps".format(self.token_object.domain, self.dev)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            break

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        self.allApps = {app['mdmName'] : app for app in query['hits']}
        self.totalHits = query["totalHits"]


    def getByName(self, appName,  entitySpace = 'PRODUCTION'):

        self.appName = appName
        self.entitySpace = entitySpace
        self.querystring = {"entitySpace": entitySpace}

        while True:
            url_filter = "https://{}.carol.ai{}/api/v1/tenantApps/name/{}".format(self.token_object.domain,
                                                                                  self.dev,
                                                                                  self.appName)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            self.appName = None
            break

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        self.appId = query['mdmId']
        self.app = {self.appName : query}

    def getById(self, appId,  entitySpace = 'PRODUCTION'):

        self.appId = appId
        self.entitySpace = entitySpace
        self.querystring = {"entitySpace": entitySpace}

        while True:
            url_filter = "https://{}.carol.ai{}/api/v1/tenantApps/{}".format(self.token_object.domain,
                                                                                  self.dev,
                                                                                  self.appId)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            self.appId = None
            break

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        self.appName = query.get('mdmName')
        self.appId = query.get('mdmId')
        self.app = {self.appName : query}

    def getSettings(self,appName = None, appId =None,  entitySpace = 'PRODUCTION', checkAllSpaces = False):

        assert appName or appId
        self.appId = appId
        self.appName = appName
        self.entitySpace = entitySpace
        self.checkAllSpaces = checkAllSpaces

        if self.appId is not None:
            self.getById(self.appId, self.entitySpace)
        else:
            self.getByName(self.appName, self.entitySpace)


        self.querystring = {"entitySpace":  self.entitySpace, "checkAllSpaces":  self.checkAllSpaces}

        while True:
            url_filter = "https://{}.carol.ai{}/api/v1/tenantApps/{}/settings".format(self.token_object.domain,
                                                                                  self.dev,
                                                                                  self.appId)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            break

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)

        self.appSettings = {}
        self.fullSettings = {}

        if not isinstance(query,list):
            query = [query]
        for query_list in query:
            self.appSettings.update({i['mdmName'] : i.get('mdmParameterValue')
                                     for i in query_list.get('mdmTenantAppSettingValues')})
            self.fullSettings.update({i['mdmName'] : i for i in query_list.get('mdmTenantAppSettingValues')})



