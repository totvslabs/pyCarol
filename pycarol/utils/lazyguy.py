from .. import entityTemplateCarol as ett
from .. import entity_template_types
from .. staging import Staging as stg
from ..verticals import Verticals
import random
import json

class lazeDM(object):
    def __init__(self,carol):
        self.carol = carol


    def start(self,json_sample,mdmName, publish = True, profileTitle = None ,overwrite = True, mdmVerticalIds=None, mdmVerticalNames='retail', mdmEntityTemplateTypeIds=None,
               mdmEntityTemplateTypeNames='product', mdmLabel=None, mdmGroupName='Others',
               mdmTransactionDataModel=False):

        if publish:
            assert  profileTitle in json_sample

        if ((mdmVerticalNames is None) and (mdmVerticalIds is None)):
            self.verticalsNameIdsDict = Verticals(self.carol).getAll()
            mdmVerticalNames = random.choice(self.verticalsNameIdsDict.keys())
            mdmVerticalIds = self.verticalsNameIdsDict.get(mdmVerticalNames)
        if ((mdmEntityTemplateTypeIds is None) and (mdmEntityTemplateTypeNames is None)):
            self.entityTemplateTypesDict = entity_template_types.entityTemplateTypeIds(self.carol).getAll()
            mdmEntityTemplateTypeNames = random.choice(self.entityTemplateTypesDict.keys())
            mdmEntityTemplateTypeIds = self.entityTemplateTypesDict.get(mdmEntityTemplateTypeNames)


        tDM = ett.createTemplate(self.carol)
        tDM.create(mdmName,overwrite=overwrite, mdmVerticalIds=mdmVerticalIds, mdmVerticalNames=mdmVerticalNames,
                   mdmEntityTemplateTypeIds=mdmEntityTemplateTypeIds,mdmEntityTemplateTypeNames=mdmEntityTemplateTypeNames,
                   mdmLabel=mdmLabel, mdmGroupName=mdmGroupName,   mdmTransactionDataModel=mdmTransactionDataModel)

        entityTemplateId = tDM.template_dict[mdmName]['mdmId']

        tDM.from_json(json_sample, profileTitle= profileTitle, publish= publish, entityTemplateId=entityTemplateId)
