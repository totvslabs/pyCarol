from unittest.mock import patch, MagicMock, PropertyMock
from ..query import Query

""" 
# Task Execution
    When testing a Task execution, there a couple of things that could be tested:
        - Execution success
        - Requires
        - Execution

# Mocks


# Special Words when defining Test Cases:

    * Test
    Every test case must start with the 'Test' keyword as default from python.unittest.
    
    * Carol
    If your test case involves Carol related tasks, use the Carol keyword. E.g. TestCarolMyTest. This way, Carol updates
    will be able to be checked. 
    
"""

class mock_carol_query:
    def __init__(self, *mock_queries):
        """
        :param mock_queries: list of dicts
        """
        self.mock_queries = mock_queries

    def __call__(self, test_func):
        """
        MOCK_QUERY:

        MOCK_DM_QUERY: pass a datamodel so any query with "mdmValue" = MOCK_DM_QUERY + "Golden" will be mocked.

        QUERY_RESPONSE:

        MAX_HITS:

        RESPONSE_TYPE:
        It is expected that each query response follows a compact type, i.e. that query results should be similar to the
        results output from pyCarol.query.Query.go

        If, however, one wants to have the query on the format exactly as it comes from Carol, it is necessary to set a
        parameter 'response_type' to be equal to 'carol'.

        E.g.:
        query = { ... } # Query to be mocked
        resp = { "mdmFieldAndValues":... } # Expected result as it comes from Carol

        @mock_carol_query(dict(mock_query=query, query_response=resp, response_type='carol'))
        def test....

        :param test_func:
        :return:
        """
        # first get a reference to the original unbound method we want to mock
        original_go = Query.go
        mock_queries = self.mock_queries

        # then create a wrapper whose main purpose is to record a reference to `self`
        # when it will be passed, then delegates the actual work to the unbound method
        def side_fx(self, *a, **kw):
            side_fx.self = self
            for dic in mock_queries:
                mock_query = None
                mock_dm_query = None
                if 'mock_query' in dic:
                    mock_query = dic['mock_query']
                if 'mock_dm_query' in dic:
                    mock_dm_query = dic['mock_dm_query']
                if 'query_response' in dic:
                    query_resp = dic['query_response']
                if 'max_hits' in dic:
                    self.max_hits = dic['max_hits']
                response_type = 'compact'  # Default value for response_type
                if 'response_type' in dic:
                    response_type = dic['response_type']

                replace_query = False
                if mock_query is not None:
                    if self.json_query == mock_query:
                        replace_query = True
                elif mock_dm_query is not None:
                    if self.json_query.get('mustList')[0].get('mdmValue') == mock_dm_query+'Golden':
                        replace_query = True

                if replace_query:
                    class QueryResponse:
                        # get mdmGoldenFieldAndValues if not empty and if it exists
                        if response_type == 'carol':
                            results = [elem.get('mdmGoldenFieldAndValues', elem)
                                       for elem in query_resp if elem.get('mdmGoldenFieldAndValues', None)]
                        elif response_type == 'compact':
                            results = query_resp

                    return QueryResponse()
            return original_go(self, *a, **kw)

        @patch('pycarol.query.Query.go', autospec=True, side_effect=side_fx)
        def mocked_test(self, patched, *args, **kwargs):
            test_func(self, *args, **kwargs)

        return mocked_test #TODO What if mocked_test has args?


def mock_carol_app(func):
    # TODO - Mock functions related to Carol App functionalities
    pass
