from unittest import TestCase
from pycarol.luigi_extension.task import Task
from pycarol.luigi_extension.targets import DummyTarget
from pycarol.luigi_extension import PickleLocalTarget
import logging
logger = logging.getLogger(__name__)


class TestTarget(TestCase):
    def test_old_style_raises_warning(self):

        class SampleTask(Task):
            TARGET = DummyTarget

        self.assertWarns(DeprecationWarning, SampleTask().output)
        self.assertEqual(SampleTask().output().__class__, DummyTarget)
        # TODO Test target content - luigi execution

    def test_new_style_works(self):
        class SampleTask(Task):
            target_type = DummyTarget

        class NoChangesTask(Task):
            pass

        self.assertEqual(SampleTask().output().__class__, DummyTarget)
        self.assertEqual(NoChangesTask().output().__class__, PickleLocalTarget)
        # TODO Test target content - luigi execution
