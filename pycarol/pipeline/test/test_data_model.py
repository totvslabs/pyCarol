import unittest
from ..data_model import DataModel, Field
from unittest.mock import patch
from .assets import datamodel_mock
from importlib import reload


datamodelmock_carol = {'mdmName': 'datamodelmock',
                       'mdmFields': [
                           {
                               'mdmName': 'nestedone',
                               'mdmMappingDataType': 'NESTED',
                               'mdmFields': [
                                   {
                                       'mdmName': 'nestedonedeep',
                                       'mdmMappingDataType': 'LONG',
                                       'mdmFields': []
                                   }
                               ]
                           },
                           {
                               'mdmName': 'nestedtwo',
                               'mdmMappingDataType': 'DOUBLE',
                               'mdmFields': []
                           }
                       ]}


class TestDataModel(unittest.TestCase):
    def setUp(self):
        reload(datamodel_mock)
        self.Datamodelmock = datamodel_mock.Datamodelmock

    def test_get_fields(self):
        fields = self.Datamodelmock.get_fields()
        self.assertEqual(len(fields), 2)
        self.assertEqual(set(list(fields.keys())), set(['Nestedone', 'Nestedtwo']))

    def test_interpret_datamodel_defined_on_code(self):
        carol_dm = self.Datamodelmock.to_carol()
        self.assertDictEqual(carol_dm, datamodelmock_carol)

    @patch('pycarol.data_models.data_model_build.DataModelBuild.get_dm', return_value=datamodelmock_carol)
    def test_interpret_datamodel_defined_at_carol(self, mocked):
        carol_dm = DataModel.from_carol(None)
        self.assertEqual(set(carol_dm.__dict__.keys()), set(self.Datamodelmock.__dict__.keys()))

    def test_generate_validation_rules(self):
        pass


# Receives inputs[0] from an Igestion Task, returns success, logs
class TestDataModelValidation(unittest.TestCase):
    pass