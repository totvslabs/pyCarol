import unittest
from ..uniform_data_rule import MdmUniformType
import numpy as np


class TestValidate(unittest.TestCase):

    def test_validation_is_correct(self):
        rule = MdmUniformType(name='test', description='Test rule',
                       values={'custom': ['a', 'b', 'c']})
        data = np.array(['a', 'a', 'a', 'a', 'b', 'b', 'c'])
        success, logs = rule.validate(data)
        self.assertTrue(success)

        data = np.array(['a', 'a', 'a', 'a', 'b', 'b', 'd'])
        success, logs = rule.validate(data)
        self.assertFalse(success)
        self.assertEqual(logs['msg'], "Wrong data: ['d']")

        data = np.array(['a', 'a', 'a', 'a', 'b', 'b', 'd', 'e', 'f', 'g'])
        success, logs = rule.validate(data)
        self.assertFalse(success)
        self.assertEqual(logs['msg'], "Wrong data: ['d' 'e' 'f' 'g']")