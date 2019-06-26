import unittest
from pycarol.auth.ApiKeyAuth import ApiKeyAuth

class ApiKeyAuthTestCase(unittest.TestCase):
    
    def test_set_connectorid(self):
        auth = ApiKeyAuth('6e423460919411e8855c3a7feb81f191')
        auth.set_connector_id('cf9446a00d8611e89f210242ac110003')
        
        self.assertEqual(auth.connector_id, 'cf9446a00d8611e89f210242ac110003')
        
    def test_authenticate_request_test(self):
        headers = {'x-auth-key':'', 'x-auth-connectorid':''}
        auth = ApiKeyAuth('6e423460919411e8855c3a7feb81f191')
        auth.set_connector_id('cf9446a00d8611e89f210242ac110003')
        auth.authenticate_request(headers)
        
        self.assertEqual(headers['x-auth-key'], '6e423460919411e8855c3a7feb81f191')
        self.assertEqual(headers['x-auth-connectorid'], 'cf9446a00d8611e89f210242ac110003')

        
if __name__ == '__main__':
    unittest.main()
    
    
    

    