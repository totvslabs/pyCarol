from .. import entityTemplateCarol as ett
from .. import entityTemplateTypesCarol
from .. import stagingCarol as stg
from ..verticalsCarol import verticals
import random
import json

class lazeDM(object):
    def __init__(self,token_object):
        self.token_object = token_object
        if self.token_object.access_token is None:
            self.token_object.newToken()

        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}

    def start(self,json_sample,mdmName, publish = True, profileTitle = None ,overwrite = True, mdmVerticalIds=None, mdmVerticalNames='retail', mdmEntityTemplateTypeIds=None,
               mdmEntityTemplateTypeNames='product', mdmLabel=None, mdmGroupName='Others',
               mdmTransactionDataModel=False):

        if publish:
            assert  profileTitle in json_sample

        if ((mdmVerticalNames is None) and (mdmVerticalIds is None)):
            self.verticalsNameIdsDict = verticals(self.token_object).getAll()
            mdmVerticalNames = random.choice(self.verticalsNameIdsDict.keys())
            mdmVerticalIds = self.verticalsNameIdsDict.get(mdmVerticalNames)
        if ((mdmEntityTemplateTypeIds is None) and (mdmEntityTemplateTypeNames is None)):
            self.entityTemplateTypesDict = entityTemplateTypesCarol.entityTemplateTypeIds(self.token_object).getAll()
            mdmEntityTemplateTypeNames = random.choice(self.entityTemplateTypesDict.keys())
            mdmEntityTemplateTypeIds = self.entityTemplateTypesDict.get(mdmEntityTemplateTypeNames)


        tDM = ett.createTemplate(self.token_object)
        tDM.create(mdmName,overwrite=overwrite, mdmVerticalIds=mdmVerticalIds, mdmVerticalNames=mdmVerticalNames,
                   mdmEntityTemplateTypeIds=mdmEntityTemplateTypeIds,mdmEntityTemplateTypeNames=mdmEntityTemplateTypeNames,
                   mdmLabel=mdmLabel, mdmGroupName=mdmGroupName,   mdmTransactionDataModel=mdmTransactionDataModel)

        entityTemplateId = tDM.template_dict[mdmName]['mdmId']

        tDM.from_json(json_sample, profileTitle= profileTitle, publish= publish, entityTemplateId=entityTemplateId)
