from unittest.mock import patch, MagicMock, PropertyMock
import unittest
import luigi
from luigi_extension import Task, inherit_list
from ..luigi import SettingsDefinition, Parameter, set_parameters
from ...query import Query
from ...carol import Carol
from ...auth import ApiKeyAuth

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
    
    # TODO:
        - Check significant parameters behavior
"""


class mock_carol_query:
    def __init__(self, *mock_queries):
        """
        :param mock_queries: list of dicts
        """
        self.mock_queries = mock_queries

    def __call__(self, test_func):
        """
        MOCK_QUERY: pass a query, so any query with this exact value will be mocked

        MOCK_DM_QUERY: pass a datamodel so any query with "mdmValue" = MOCK_DM_QUERY + "Golden" will be mocked.

        QUERY_RESPONSE:

        MAX_HITS: define a maximum number of hits to a query

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
                    self.get_all = False
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


class TaskMock(Task):
    TARGET_DIR = "app/data/test/targets/luigi_extension/task"


class TestLuigiExtensionParameterTask(unittest.TestCase):
    # def test_get_parameters_from_carol(self):
    #     expected_params = {"abandoned":'1_11,2_11',
    #                        "branch":'ALL',
    #                        "startdate":'01/01/2008'}
    #
    #     class P(SettingsDefinition):
    #         abandoned = Parameter(carol=True, carol_name='abandonedenrollment', description='')
    #         branch = Parameter(carol=True, carol_name='subsidiary', description='')
    #         startdate = Parameter(carol=True, carol_name='initialdate', description='')
    #
    #     app_config = test_config.config[TEST_APP_TENANT]
    #     P.app = {}
    #     P.app_name = app_config['APP_NAME']
    #     P.api_token = Carol(TEST_APP_TENANT,
    #                         app_config['APP_NAME'],
    #                         auth=ApiKeyAuth(app_config['X_AUTH_KEY']),
    #                         connector_id=app_config['X_CONNECTOR_ID'])
    #     set_parameters.get_app_params(P)
    #     self.assertEqual(expected_params, P.app)

    def test_task_gets_parameters_from_all_inherited_tasks_and_set_them_to_what_is_in_ParameterTask(self):

        test_list = []

        class TaskA1(luigi.Task):
            param_a1 = Parameter()
            task_complete = False

            def run(self):
                test_list.append(('a', self.param_a1))
                self.task_complete = True

            def complete(self):
                return self.task_complete

        class TaskA2(luigi.Task):
            param_a2 = Parameter()
            task_complete = False

            def run(self):
                test_list.append(('a', self.param_a2))
                self.task_complete = True

            def complete(self):
                return self.task_complete

        @inherit_list((TaskA1, dict(param_a1=4)))
        class TaskB(TaskMock):
            param_b = Parameter()
            task_complete = False

            def run(self):
                test_list.append(('b', self.param_b))
                self.task_complete = True

            def complete(self):
                return self.task_complete

        @inherit_list(TaskB)
        class TaskC(TaskMock):
            param_c = Parameter()
            task_complete = False

            def run(self):
                test_list.append(('c', self.param_c))
                self.task_complete = True

            def complete(self):
                return self.task_complete

        @inherit_list(TaskA2)
        class TaskD(TaskMock):
            param_c = Parameter()
            task_complete = False

            def run(self):
                test_list.append(('d', self.param_c))
                self.task_complete = True

            def complete(self):
                return self.task_complete

        class AppParams(SettingsDefinition):
            TARGET_DIR = "app/data/test/targets/luigi_extension"
            param_a1 = Parameter(default=1)
            param_a2 = Parameter(default=2)
            param_b = Parameter(default=3)
            param_c = Parameter(default=5)

        AppParams.api_token = None

        @set_parameters(AppParams)
        class Run(luigi.WrapperTask):
            task = Parameter()

            def requires(self):
                #AppParams.copy_tasks_parameters(Run)
                return [self.clone(TaskC), self.clone(TaskD)]

        task_exec = luigi.build([Run(task='test')], local_scheduler=True)
        self.assertTrue(task_exec)
        self.assertEqual(set(test_list), set([('a', 4), ('b', 3), ('c', 5), ('a', 2), ('d', 5)]))