import unittest
import os
import sys
sys.path.append('../.')
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol
from pycarol.query import Query
from pycarol.filter import Filter

def carol_constructor():
    return Carol(domain='rui',
              app_name='',
              auth=ApiKeyAuth('6e423460919411e8855c3a7feb81f191'),
              connector_id='b0afa34067f111e8989cee82274893da')

class queryCallback():
    def __init__(self):
        pass
    def __call__(self, data):
        self.called = True

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
        
        search_query = Filter.Builder().type("customermanufacturingGolden").build().to_json()
        
        query_result = Query(carol, page_size=1000, print_status=False).query(search_query).go()
        self.assertTrue(len(query_result.results) > 0)
    
    
    def test_go_only_hits_false_query(self):
        carol = carol_constructor()
        
        search_query = Filter.Builder().type("customermanufacturingGolden").build().to_json()

        query_result = Query(carol, page_size=2, max_hits=1, only_hits=False, print_status=False).query(search_query).go()
        self.assertTrue(len(query_result.results) > 0)
    
    
    def test_go_save_results_query(self):
        carol = carol_constructor()
        
        search_query = Filter.Builder().type("customermanufacturingGolden").build().to_json()

        if os.path.isfile('./query_result.json'):
            os.remove('./query_result.json')
        
        query_result = Query(carol, page_size=2, max_hits=1, filename='query_result.json', save_results=True, print_status=False).query(search_query).go()
        self.assertTrue(len(query_result.results) > 0)
        self.assertTrue(os.path.isfile('./query_result.json'))
    
    
    def test_go_named_query(self):
        carol = carol_constructor()
        query_result = Query(carol, page_size=2, max_hits=1, print_status=False).named('queryForUnitTest').go()
        self.assertTrue(len(query_result.results) > 0)
    
    
    def test_go_callback_query(self):
        carol = carol_constructor()
        callback = queryCallback()
        
        search_query = Filter.Builder().type("customermanufacturingGolden").build().to_json()

        query_result = Query(carol, page_size=1000, print_status=False).query(search_query).go(callback=callback)
        self.assertTrue(callback.called)
    
if __name__ == '__main__':
    unittest.main()

    
    