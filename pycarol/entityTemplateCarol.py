import requests
import json
from .entityTemplateTypesCarol import *
from .verticalsCarol import *
from .fieldsCarol import *


class createTemplate(object):
    def __init__(self, token_object):
        self.token_object = token_object
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.templates_dict = {}

    def _checkVerticals(self):
        self.verticalsNameIdsDict = verticals(self.token_object).getAll()

        if self.mdmVerticalIds is not None:
            for key, value in self.verticalsNameIdsDict.items():
                if value == self.mdmVerticalIds:
                    self.mdmVerticalIds = value
                    self.mdmVerticalNames = key
                    return
        else:
            for key, value in self.verticalsNameIdsDict.items():
                if key == self.mdmVerticalNames:
                    self.mdmVerticalIds = value
                    self.mdmVerticalNames = key
                    return

        raise Exception('{}/{} are not valid values for mdmVerticalNames/mdmVerticalIds./n'
                        ' Possible values are: {}'.format(self.mdmVerticalNames, self.mdmVerticalIds,
                                                          self.verticalsNameIdsDict))

    def _checkEntityTemplateTypes(self):
        self.entityTemplateTypesDict = entityTemplateTypeIds(self.token_object).getAll()

        if self.mdmEntityTemplateTypeIds is not None:
            for key, value in self.entityTemplateTypesDict.items():
                if value == self.mdmEntityTemplateTypeIds:
                    self.mdmEntityTemplateTypeIds = value
                    self.mdmEntityTemplateTypeNames = key
                    return
        else:
            for key, value in self.entityTemplateTypesDict.items():
                if key == self.mdmEntityTemplateTypeNames:
                    self.mdmEntityTemplateTypeIds = value
                    self.mdmEntityTemplateTypeNames = key
                    return

        raise Exception('{}/{} are not valid values for mdmEntityTemplateTypeNames/mdmEntityTemplateTypeIds./n'
                        ' Possible values are: {}'.format(self.mdmVerticalNames, self.mdmVerticalIds,
                                                          self.entityTemplateTypesDict))

    def create(self, mdmName, mdmVerticalIds=None, mdmVerticalNames=None, mdmEntityTemplateTypeIds=None,
               mdmEntityTemplateTypeNames=None, mdmLabel=None, mdmGroupName='Others',
               mdmTransactionDataModel=False):

        self.mdmName = mdmName
        self.mdmGroupName = mdmGroupName

        if not mdmLabel:
            self.mdmLabel = self.mdmName
        else:
            self.mdmLabel = mdmLabel


        self.mdmTransactionDataModel = mdmTransactionDataModel

        assert ((mdmVerticalNames) or (mdmVerticalIds))
        assert ((mdmEntityTemplateTypeIds) or (mdmEntityTemplateTypeNames))

        self.mdmVerticalNames = mdmVerticalNames
        self.mdmVerticalIds = mdmVerticalIds
        self.mdmEntityTemplateTypeIds = mdmEntityTemplateTypeIds
        self.mdmEntityTemplateTypeNames = mdmEntityTemplateTypeNames

        self._checkVerticals()
        self._checkEntityTemplateTypes()

        payload = {"mdmName": self.mdmName, "mdmGroupName": self.mdmGroupName, "mdmLabel": {"en-US": self.mdmLabel},
                   "mdmVerticalIds": [self.mdmVerticalIds],
                   "mdmEntityTemplateTypeIds": [self.mdmEntityTemplateTypeIds],
                   "mdmTransactionDataModel": self.mdmTransactionDataModel, "mdmProfileTitleFields": []}

        while True:
            url_filter = "https://{}.carol.ai/api/v1/entities/templates".format(self.token_object.domain)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, json = payload)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token,
                                    'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            break

        self.lastResponse.encoding = 'utf8'
        response = [json.loads(self.lastResponse.text)]
        self.templates_dict.update({response['mdmName']: response})



    def addFields(self,json_sample):

        fields = fieldsCarol(self.token_object)
        fields.possibleTypes()
        self.possible_types = fields._possibleTypes
        #fields.getAll()
        self.all_possible_fields = fields.fields_dict

        #for prop, value in json_sample.items():
        #    pass



"""
##############
############################
############################
############################
##############

#To add fields, firts need to cread the field.  (OBS. there are a lot of pre created fields to be used.)

url = "https://mario.carol.ai/api/v1/fields"

payload = {"mdmName":"nome","mdmMappingDataType":"string","mdmFieldType":"PRIMITIVE","mdmLabel":{"en-US":"Nome"},"mdmDescription":{"en-US":"Nome"}}
headers = {
    'authorization': "2561c210b11611e78cce0242ac110003",
    'content-type': "application/json;charset=UTF-8",
    'accept': "application/json, text/plain, */*",
    }

response = requests.request("POST", url, json=payload, headers=headers)


#response:
''' 
{"mdmName":"nome","mdmLabel":{"en-US":"Nome"},"mdmDescription":{"en-US":"Nome"},"mdmIndex":"ANALYZED","mdmAnalyzer":"STANDARD",
 "mdmMappingDataType":"STRING","mdmFields":[],"mdmForeignKeyField":false,"mdmFieldsFull":{},"mdmFieldType":"PRIMITIVE","mdmTags":[],
 "mdmId":"73792f30b11911e78cce0242ac110003","mdmEntityType":"mdmField","mdmCreated":"2017-10-14T19:54:12Z",
 "mdmLastUpdated":"2017-10-14T19:54:12Z","mdmTenantId":"8d45c660a85b11e7a2900242ac110003"}
'''


#a nested field

import requests

url = "https://mario.carol.ai/api/v1/fields"

payload = {"mdmName":"nestedtest","mdmMappingDataType":"nested","mdmFieldType":"NESTED",
           "mdmLabel":{"en-US":"nested test"},"mdmDescription":{"en-US":"nested"}}
headers = {
    'authorization': "2561c210b11611e78cce0242ac110003",
    'content-type': "application/json;charset=UTF-8"
    }

response = requests.request("POST", url, data=payload, headers=headers)


#response:
''' 
{"mdmName":"nestedtest","mdmLabel":{"en-US":"nested test"},"mdmDescription":{"en-US":"nested"},
 "mdmIndex":"ANALYZED","mdmAnalyzer":"STANDARD","mdmMappingDataType":"NESTED","mdmFields":[],
 "mdmForeignKeyField":false,"mdmFieldsFull":{},"mdmFieldType":"NESTED","mdmTags":[],"mdmId":"5abfc4c0b11b11e78cce0242ac110003",
 "mdmEntityType":"mdmField","mdmCreated":"2017-10-14T20:07:50Z","mdmLastUpdated":"2017-10-14T20:07:50Z",
 "mdmTenantId":"8d45c660a85b11e7a2900242ac110003"}
'''





#Then to add a field to a DM


url = "https://mario.carol.ai/api/v1/entities/templates/4d7e5010b11611e78cce0242ac110003/onboardField/73792f30b11911e78cce0242ac110003"

querystring = {"parentFieldId":""}

headers = {
    'authorization': "2561c210b11611e78cce0242ac110003",
    'accept': "application/json, text/plain, */*"
    }

response = requests.request("POST", url, headers=headers, params=querystring)

#Then to add nested a field to a DM

import requests

url = "https://mario.carol.ai/api/v1/entities/templates/4d7e5010b11611e78cce0242ac110003/onboardField/5abfc4c0b11b11e78cce0242ac110003"

querystring = {"parentFieldId":""}

headers = {
    'accept-language': "en-US,en;q=0.8,pt-BR;q=0.6,pt;q=0.4",
    'authorization': "2561c210b11611e78cce0242ac110003",
    }

response = requests.request("POST", url, headers=headers, params=querystring)



# to add a chield to the nest field to a DM

url = "https://mario.carol.ai/api/v1/entities/templates/4d7e5010b11611e78cce0242ac110003/onboardField/fcc94630b07911e793370242ac110003"

querystring = {"parentFieldId":"5abfc4c0b11b11e78cce0242ac110003"}

headers = {

    'accept-language': "en-US,en;q=0.8,pt-BR;q=0.6,pt;q=0.4",
    'authorization': "2561c210b11611e78cce0242ac110003"
    }

response = requests.request("POST", url, headers=headers, params=querystring)

print(response.text)


#Add profile title

import requests

url = "https://mario.carol.ai/api/v1/entities/templates/4d7e5010b11611e78cce0242ac110003/profileTitle"

payload = ["mdmtaxid"]  #list of fields
headers = {
    'accept-language': "en-US,en;q=0.8,pt-BR;q=0.6,pt;q=0.4",
    'authorization': "2561c210b11611e78cce0242ac110003"
    }

response = requests.request("POST", url, data=payload, headers=headers)

"""