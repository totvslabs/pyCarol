import unittest
import sys
sys.path.append('../.')
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol

def carol_constructor():
    return Carol(domain='rui',
              app_name='',
              auth=ApiKeyAuth('6e423460919411e8855c3a7feb81f191'),
              connector_id='b0afa34067f111e8989cee82274893da')


class CarolTestCase(unittest.TestCase):
    
    def test_carol_init(self):
        login = carol_constructor()
        
        self.assertTrue(isinstance(login, Carol))
    
    
    def test_call_api_get(self):
        carol = carol_constructor()
        
        path = 'v2/apiKey/connectorIds'
        method = 'GET'
        params = {'apiKey':'97b3e9c091a211e8855c3a7feb81f191' ,'connectorId':'b0afa34067f111e8989cee82274893da' }
        
        response = carol.call_api(path, method=method, content_type='application/json')
        self.assertTrue(carol.response.ok)
    
    
    def test_call_api_post(self):
        carol = carol_constructor()
        
        query = {
              "mustList": [
                {
                  "mdmFilterType": "TYPE_FILTER",
                  "mdmValue": "mdmreceiptGolden"
                }
              ]
            }
        
        path = 'v2/queries/filter'
        method = 'POST'
        params = {'pageSize':'0', 'sortOrder':'ASC', 'scrollable':'false', 'indexType':'MASTER'}
        
        result = carol.call_api(path, method=method, data=query, auth=True, params=params, content_type='application/json')
        self.assertTrue(carol.response.ok)
    
        
if __name__ == '__main__':
    unittest.main()


