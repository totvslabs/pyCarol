import logging
import os
import unittest

from pycarol.luigi_extension.kubernetestask import EasyKubernetesTask

logger = logging.getLogger(__name__)


class TestK8Tasks(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_env_vars(self):
        os.environ['CAROLCONNECTORID'] = '123'
        os.environ['CAROLAPPOAUTH'] = '1234'
        os.environ['LONGTASKID'] = 'abc'
        os.environ['CAROLTENANT'] = 'carol'
        os.environ['CAROLAPPNAME'] = 'carol-app'
        os.environ['IMAGE_NAME'] = 'busybox'

        task = EasyKubernetesTask()
        self.assertDictEqual(
            task.env_vars,
            {
                "CAROLCONNECTORID": '123',
                "CAROLAPPOAUTH": '1234',
                "LONGTASKID": 'abc',
                "CAROLTENANT": 'carol',
                "CAROLAPPNAME": 'carol-app',
                "IMAGE_NAME": 'busybox',
            })
        os.environ['CAROLCONNECTORID'] = ''
        os.environ['CAROLAPPOAUTH'] = ''
        os.environ['LONGTASKID'] = ''
        os.environ['CAROLTENANT'] = ''
        os.environ['CAROLAPPNAME'] = ''
        os.environ['IMAGE_NAME'] = ''

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
