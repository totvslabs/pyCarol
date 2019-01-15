from ..data_models.data_model_types import DataModelTypeIds
from ..verticals import Verticals
from ..data_models.data_models import CreateDataModel

import random


class DataModelGenerator(object):

    """
    This class will create a Datamodel from a python dictionary. Today, it is not allowed nested fileds in the dictionary.

    Ex:
    ```python
    from pycarol.auth.PwdAuth import PwdAuth
    from pycarol.auth.ApiKeyAuth import ApiKeyAuth
    from pycarol.carol import Carol

    from pycarol.utils.data_model_generator import DataModelGenerator

    json_sample = {'atende_sus': 'Sim',
                   'estabelecimento': 'Hospital Raimundo Chaar',
                   'existente': '42',
                   'tipo_do_estabelecimento': '05 - Hospital Geral',
                   'tipo_do_estabelecimento_resumo': 'Hospitais',
                   'uf': 'AC'}

    login = Carol('foo_tenant, 'foo', auth=PwdAuth('foo@totvs.com.br', 'foo123'))
    la = DataModelGenerator(login)
    la.start(json_sample, dm_name='hfair', profile_title='estabelecimento', publish=True, overwrite=True)
    ```

    """


    def __init__(self, carol):
        """

        :param carol: Carol object
            Carol object.
        """
        self.carol = carol

    def start(self, json_sample, dm_name, publish=True, profile_title=None, overwrite=True, vertical_ids=None,
              vertical_names='retail', entity_template_type_ids=None,
              entity_template_type_names='product', dm_label=None,
              group_name='Others', transaction_dm=False):

        """

        :param json_sample:
        :param dm_name:
        :param publish:
        :param profile_title:
        :param overwrite:
        :param vertical_ids:
        :param vertical_names:
        :param entity_template_type_ids:
        :param entity_template_type_names:
        :param dm_label:
        :param group_name:
        :param transaction_dm:
        :return:
        """

        if publish:
            assert profile_title in json_sample

        if ((vertical_names is None) and (vertical_ids is None)):
            self.verticals_dict = Verticals(self.carol).all()
            vertical_names = random.choice(self.verticals_dict.keys())
            vertical_ids = self.verticals_dict.get(vertical_names)
        if ((entity_template_type_ids is None) and (entity_template_type_names is None)):
            self.entityTemplateTypesDict = DataModelTypeIds(self.carol).all()
            entity_template_type_names = random.choice(self.entityTemplateTypesDict.keys())
            entity_template_type_ids = self.entityTemplateTypesDict.get(entity_template_type_names)

        tDM = CreateDataModel(self.carol)
        tDM.create(dm_name, overwrite=overwrite, vertical_ids=vertical_ids, vertical_names=vertical_names,
                   entity_template_type_ids=entity_template_type_ids,
                   entity_template_type_names=entity_template_type_names,
                   label=dm_label, group_name=group_name, transaction_data_model=transaction_dm)

        dm_id = tDM.template_dict[dm_name]['mdmId']

        tDM.from_json(json_sample, profile_title=profile_title, publish=publish, dm_id=dm_id)
        print('Done!')
