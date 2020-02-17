"""
This tool helps to DataModels from json files.


"""

from ..data_models.data_model_types import DataModelTypeIds
from ..verticals import Verticals
from ..data_models.data_models import CreateDataModel

import random


class DataModelGenerator(object):

    """
    This class will create a Datamodel from a python dictionary. Today, it is not allowed nested fileds in the dictionary.

    Usage:

    .. code-block:: python

        from pycarol import PwdAuth
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


    """


    def __init__(self, carol):
        self.carol = carol

    def start(self, json_sample, dm_name, publish=True, profile_title=None, overwrite=False, vertical_ids=None,
              vertical_names='retail', entity_template_type_ids=None,
              entity_template_type_names='product', dm_label=None,
              group_name='Others', transaction_dm=False, ignore_field_type=False):
        """

        Start the process.

        Args:

            json_sample: `dict`
                Dictionary with key and values to be used as template for the data model
            dm_name: `str`
                Data model name
            publish: `bool` default `True`
                Publish the data model at the end.
            profile_title: `list` default `None`
                Carol's Profile title. It should be an field in json_sample
            overwrite: `bool` default `False`
                Overwrite if the data model already exists
            vertical_ids: `str` default `None`
                Vertical ID for the data model. either `vertical_ids`
            vertical_names: `str` default `retail`
                Vertical Name for the data model. either `vertical_ids`
            entity_template_type_ids: `str` default `None`
                Data model template type id
            entity_template_type_names: `str` default `product`
                Data model template type name
            dm_label: `str` default `None`
                Label for the data model
            group_name: `str` default `Others`
                Group name for the data model
            transaction_dm: `bool` default `False`
                Transaction Data model
            ignore_field_type: `bool` default `False`
                Ignore filed type when creating the data model. It means that any type conflict will be ignored. pycarol
                will use the one that already exists.

        Returns:
            None
        """

        if publish:
            if profile_title is None:
                raise ValueError('To publish the data model, `profile_title` has to be set. Use `publish=False`')
            if isinstance(profile_title, str):
                profile_title = [profile_title]
            elif not isinstance(profile_title, list):
                raise ValueError('`profile_title` has to be a list of values.')
            assert all([i in json_sample for i in profile_title]), "all profile title values should be in `json_sample`"

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

        tDM.from_json(json_sample, profile_title=profile_title, publish=publish, dm_id=dm_id,
                      ignore_field_type=ignore_field_type, )
        print('Done!')
