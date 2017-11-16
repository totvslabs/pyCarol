import json
import requests
#from . import utils

class connectorsCarol:
    def __init__(self, token_object):

        self.token_object = token_object

        self.groupName = "Others"

        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}

    def createConnector(self,connectorName, connectorLabel = None, groupName = "Others"):
        self.connectorName = connectorName
        if connectorLabel is None:
            self.connectorLabel = self.connectorName
        else:
            self.connectorLabel = connectorLabel

        self.groupName = groupName
        url = "https://{}.carol.ai/api/v1/applications".format(self.token_object.domain)


        payload = {  "mdmName": self.connectorName,   "mdmGroupName": self.groupName,   "mdmLabel": { "en-US": self.connectorLabel }}


        while True:
            self.response = requests.request("POST", url, json=payload,headers=self.headers)
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
        print('Connector created: connector ID = {}'.format(self.connectorId))


    def deleteConnector(self,connectorId, forceDeletion = True):
        self.connectorId = connectorId
        url = "https://{}.carol.ai/api/v1/applications/{}".format(self.token_object.domain,self.connectorId)
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
        url = "https://{}.carol.ai/api/v1/applications/name/{}".format(self.token_object.domain,self.connectorName)

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

