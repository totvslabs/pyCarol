import unittest
from unittest.mock import patch
from .test import test_task_execution
from ..task import Task


class FakeTarget:
    FILE_EXT = ''

    def exists(self):
        return True

    def __init__(self, *args, **kwargs):
        pass


class TestTaskSetParameter(unittest.TestCase):

    @patch('luigi_extension.task.SqliteTarget')
    def test_target_change(self, mocked):
        class TestTask(Task):
            TARGET = FakeTarget

        mocked.return_value = FakeTarget()
        out = test_task_execution(TestTask)
        self.assertTrue(out.success)
        self.assertEqual(out.task.output().__class__.__name__, 'FakeTarget')

    @patch('luigi_extension.task.SqliteTarget')
    def test_future_deprecation_set_parameters_method_works_as_a_target_type_definition(self, mocked):
        class TestTask(Task):
            set_target = Task.sqlite_target

        mocked.return_value = FakeTarget()
        out = test_task_execution(TestTask)
        self.assertTrue(out.success)
        self.assertEqual(out.task.output().__class__.__name__, 'FakeTarget')