
import unittest
import pandas as pd
from ..field import Field
from ..validation_rule.validation_rule import ValidationRule
from ..validation_rule.uniform_data_rule import MdmUniformType
from unittest.mock import patch


class Fieldmock(Field):
    TYPE = Field.TYPE.NESTED

    class Nestedone(Field):
        TYPE = Field.TYPE.NESTED

        class Nestedonedeep(Field):
            TYPE = Field.TYPE.LONG

            @classmethod
            def custom_validation(cls, data):
                # Data must be greater than 5
                return not any(data > 5)

    class Nestedtwo(Field):
        TYPE = Field.TYPE.DOUBLE

        @classmethod
        def uniform_rule(cls):
            return MdmUniformType.get_rule('test')

    class Nestedthree(Field):
        TYPE = Field.TYPE.DOUBLE

        @classmethod
        def validation_rule(cls):
            return ValidationRule(ValidationRule.OPERATION.EQUALS, [1])


fieldmock_carol_code = {
            'mdmName':'fieldmock',
            'mdmMappingDataType':Field.TYPE.NESTED,
            'mdmFields': [
                 {
                     'mdmName': 'nestedone',
                     'mdmMappingDataType': Field.TYPE.NESTED,
                     'mdmFields':[
                         {
                             'mdmName':'nestedonedeep',
                             'mdmMappingDataType':Field.TYPE.LONG,
                             'mdmFields': []
                         }
                     ]
                 },
                 {
                     'mdmName': 'nestedtwo',
                     'mdmMappingDataType': Field.TYPE.DOUBLE,
                     'mdmFields': []
                 },
                {
                    'mdmName': 'nestedthree',
                    'mdmMappingDataType': Field.TYPE.DOUBLE,
                    'mdmFields': []
                }
             ]
            }


class TestField(unittest.TestCase):

    def test_interpret_fields_defined_on_code(self):
        self.assertDictEqual(Fieldmock.to_carol(), fieldmock_carol_code)

    def test_interpret_fields_defined_at_carol(self):
        FromCarol = Field.from_carol(fieldmock_carol_code)
        self.assertEqual(set(FromCarol.__dict__.keys()), set(Fieldmock.__dict__.keys()))
        self.assertEqual(set(FromCarol.Nestedone.__dict__.keys()), set(Fieldmock.Nestedone.__dict__.keys()))


    def test_get_fields(self):
        self.assertEqual(set(Fieldmock.get_fields().values()), {Fieldmock.Nestedone, Fieldmock.Nestedtwo,
                                                                 Fieldmock.Nestedthree})

    def test_get_field_parent(self):
        parent = Fieldmock.Nestedone.Nestedonedeep.get_parent()
        self.assertEquals(parent, Fieldmock.Nestedone)


class TestFieldValidation(unittest.TestCase):

    def test_type_validation(self):
        data = pd.DataFrame({'nestedonedeep': ['a', 'b', 'c', 'd', 'e', 'f']})
        success, logs = Fieldmock.Nestedone.Nestedonedeep.validate(data, ignore_errors=True)
        self.assertFalse(success)
        data = pd.DataFrame({'nestedonedeep': [-1, 0, 1, 2, 3, 4]})
        success, logs = Fieldmock.Nestedone.Nestedonedeep.validate(data, ignore_errors=False)
        self.assertTrue(success)

    def test_custom_validation(self):
        data = pd.DataFrame({'nestedonedeep': [1, 2, 3, 4, 5, 6]})
        success, logs = Fieldmock.Nestedone.Nestedonedeep.validate(data, ignore_errors=True)
        self.assertFalse(success)
        data = pd.DataFrame({'nestedonedeep': [-1, 0, 1, 2, 3, 4]})
        success, logs = Fieldmock.Nestedone.Nestedonedeep.validate(data, ignore_errors=False)
        self.assertTrue(success)

    def test_validation_rule_validation(self):
        data = pd.DataFrame({'nestedthree': [1., 2., 3., 4., 5., 6.]})
        success, logs = Fieldmock.Nestedthree.validate(data, ignore_errors=True)
        self.assertFalse(success)
        data = pd.DataFrame({'nestedthree': [1., 1., 1., 1., 1., 1.]})
        success, logs = Fieldmock.Nestedthree.validate(data, ignore_errors=False)
        self.assertTrue(success)

    @patch('pycarol.pipeline.validation_rule.uniform_data_rule.MdmUniformType.get_rule',
           return_value=MdmUniformType('test', 'Test description', version='1.0.0', values={'a': [1., 2., 3.]}))
    def test_uniform_rule_validation(self, mocked):
        data = pd.DataFrame({'nestedtwo': [1., 2., 3., 4., 5., 6.]})
        success, logs = Fieldmock.Nestedtwo.validate(data, ignore_errors=True)
        self.assertFalse(success)
        data = pd.DataFrame({'nestedtwo': [1., 1., 2., 2., 3., 3.]})
        success, logs = Fieldmock.Nestedtwo.validate(data, ignore_errors=False)
        self.assertTrue(success)

    @patch('pycarol.pipeline.validation_rule.uniform_data_rule.MdmUniformType.get_rule',
           return_value=MdmUniformType('test', 'Test description', version='1.0.0', values={'a': [1., 2., 3.]}))
    def test_all_validation(self, mocked):
        data = pd.DataFrame({
            'fieldmock_nestedone_nestedonedeep':['a', 'b', 'c', 'd', 'e', 'f'],
            'fieldmock_nestedtwo': [1, 2, 3, 4, 5, 6],
            'fieldmock_nestedthree': [1., 2., 3., 4., 5., 6.]
        })
        success, logs = Fieldmock.validate(data, ignore_errors=True)
        self.assertFalse(success)

        data = pd.DataFrame({
            'nestedone_nestedonedeep': [1, 2, 3, 4, 5, 6],
            'nestedtwo': [1., 1., 2., 2., 3., 3.],
            'nestedthree': [1., 1., 1., 1., 1., 1.]
        })
        success, logs = Fieldmock.Nestedtwo.validate(data, ignore_errors=False)
        self.assertTrue(success)

    # TODO: Test logs
