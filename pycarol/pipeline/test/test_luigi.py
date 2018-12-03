
import unittest
from ..data_model import DataModel, Field


class DataModelMock(DataModel):
    class Nestedone(Field):
        TYPE = Field.TYPE.NESTED

        class Nestedonedeep(Field):
            TYPE = Field.TYPE.LONG


class TestLuigiTasks(unittest.TestCase):
    def test_validation_tasks_are_created(self):
        pass
