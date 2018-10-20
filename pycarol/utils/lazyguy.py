from ..entity_template_types import EntityTemplateTypeIds
from ..verticals import Verticals
from ..data_models import CreateDataModel

import random
import json


class lazeDM(object):
    def __init__(self, carol):
        self.carol = carol

    def start(self, json_sample, dm_name, publish=True, profile_title=None, overwrite=True, vertical_ids=None,
              vertical_names='retail', entity_template_type_ids=None,
              entity_template_type_names='product', dm_label=None,
              group_name='Others', transaction_dm=False):

        if publish:
            assert profile_title in json_sample

        if ((vertical_names is None) and (vertical_ids is None)):
            self.verticals_dict = Verticals(self.carol).all()
            vertical_names = random.choice(self.verticals_dict.keys())
            vertical_ids = self.verticals_dict.get(vertical_names)
        if ((entity_template_type_ids is None) and (entity_template_type_names is None)):
            self.entityTemplateTypesDict = EntityTemplateTypeIds(self.carol).all()
            entity_template_type_names = random.choice(self.entityTemplateTypesDict.keys())
            entity_template_type_ids = self.entityTemplateTypesDict.get(entity_template_type_names)

        tDM = CreateDataModel(self.carol)
        tDM.create(dm_name, overwrite=overwrite, vertical_ids=vertical_ids, vertical_names=vertical_names,
                   entity_template_type_ids=entity_template_type_ids,
                   entity_template_type_names=entity_template_type_names,
                   dm_label=dm_label, group_name=group_name, transaction_dm=transaction_dm)

        dm_id = tDM.template_dict[dm_name]['mdmId']

        tDM.from_json(json_sample, profile_title=profile_title, publish=publish, dm_id=dm_id)
