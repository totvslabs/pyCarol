import unittest
import sys
sys.path.append('../.')
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol
from pycarol.query import Query

def carol_constructor():
    return Carol(domain='rui',
              app_name='',
              auth=ApiKeyAuth('6e423460919411e8855c3a7feb81f191'),
              connector_id='b0afa34067f111e8989cee82274893da')


class QueryTestCase(unittest.TestCase):    
    def test_build_query_params_default(self):
        carol = carol_constructor()

        query = Query(carol)
        query._build_query_params()
        
        self.assertTrue(isinstance(query.query_params, dict))
        
    def test_build_query_params_setting_all_parameters(self):
        carol = carol_constructor()
        
        offset = 50
        page_size = 100
        sort_order = 'DES'
        index_type = 'MASTER'
        sort_by = 'FIELD'
        scrollable = True
        fields = ['FIELD1', 'FIELD2', 'FIELD3']
        
        query = Query(carol, offset=offset, page_size=page_size, sort_order=sort_order, index_type=index_type,
                     sort_by=sort_by, scrollable=scrollable, fields=fields)
        query._build_query_params()
        
        self.assertEqual(query.query_params['offset'], offset)
        self.assertEqual(query.query_params['pageSize'], page_size)
        self.assertEqual(query.query_params['sortOrder'], sort_order)
        self.assertEqual(query.query_params['indexType'], index_type)
        self.assertEqual(query.query_params['sortBy'], sort_by)
        self.assertEqual(query.query_params['scrollable'], scrollable)
        self.assertEqual(query.query_params['fields'], fields)
        
        
    def test_build_return_fields(self):
        carol = carol_constructor()
        fields_list = ['FIELD1', 'FIELD2', 'FIELD3']
        fields_str = 'FIELD1,FIELD2,FIELD3'
        
        query = Query(carol, fields = fields_list)
        query._build_return_fields()
        
        self.assertEqual(query.fields, fields_str )
        
        query = Query(carol, fields = fields_str)
        query._build_return_fields()
        
        self.assertEqual(query.fields, fields_str )
        
        
    def test_go_filter_query(self):
        carol = carol_constructor()
        
        search_query = {
                  "mustList": [
                    {
                      "mdmFilterType": "TYPE_FILTER",
                      "mdmValue": "customermanufacturingGolden"
                    }
                  ]
                }
        
        query_result = Query(carol, page_size=1000).query(search_query).go()
        self.assertTrue(len(query_result.results) > 0)
    
    
    def test_go_only_hits_false_query(self):
        carol = carol_constructor()
        
        search_query = {
                  "mustList": [
                    {
                      "mdmFilterType": "TYPE_FILTER",
                      "mdmValue": "customermanufacturingGolden"
                    }
                  ]
                }
        
        query_result = Query(carol, page_size=2, max_hits=1, only_hits=False).query(search_query).go()
        #self.assertTrue(len(query_result.results) > 0)
        print(query_result.results)
    
    
    # TODO:
    #def test_go_callback_query(self):
    #def test_go_save_results_query(self):
    #def test_go_named_query(self):
    
if __name__ == '__main__':
    unittest.main()

    
    