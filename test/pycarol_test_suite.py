import unittest
from auth_test import ApiKeyAuthTestCase
from carol_test import CarolTestCase
from query_test import QueryTestCase
def suite_pycarol():
    suite = unittest.TestSuite()
    # Test - Auth
    suite.addTest(ApiKeyAuthTestCase('test_set_connectorid'))
    suite.addTest(ApiKeyAuthTestCase('test_authenticate_request_test'))
    
    # Test - Carol
    suite.addTest(CarolTestCase('test_carol_init'))
    suite.addTest(CarolTestCase('test_call_api_get'))
    suite.addTest(CarolTestCase('test_call_api_post'))
    
    # Test - Query
    suite.addTest(QueryTestCase('test_build_query_params_default'))
    suite.addTest(QueryTestCase('test_build_query_params_setting_all_parameters'))
    suite.addTest(QueryTestCase('test_build_return_fields'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite_pycarol())