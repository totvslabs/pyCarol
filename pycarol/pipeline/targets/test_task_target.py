from unittest import TestCase
from ..task import Task
from ..targets import DummyTarget
from .. import PickleTarget
import logging
logger = logging.getLogger(__name__)


class TestTarget(TestCase):
    def test_old_deprecated_style_raises_warning(self):

        class SampleTask(Task):
            TARGET = DummyTarget

        self.assertWarns(DeprecationWarning, SampleTask().output)
        self.assertEqual(SampleTask().output().__class__, DummyTarget)
        # TODO Test target content - luigi execution

    def test_target_type_changing_default_target(self):
        class SampleTask(Task):
            target_type = DummyTarget

        class NoChangesTask(Task):
            pass

        self.assertEqual(SampleTask().output().__class__, DummyTarget)
        self.assertEqual(NoChangesTask().output().__class__, PickleTarget)
        # TODO Test target content - luigi execution
