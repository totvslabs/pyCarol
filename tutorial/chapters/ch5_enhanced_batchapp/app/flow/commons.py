import luigi
import os
import logging
import traceback
from pycarol.pipeline import Task
from datetime import datetime
from pycarol import Carol
from pycarol.apps import Apps

# Configuring the logs
#-----------------------------------------
luigi.interface.InterfaceLogging.setup(luigi.interface.core())
logger = logging.getLogger(__name__)


# Luigi basic parameters
#-----------------------------------------
PROJECT_PATH = os.getcwd()
TARGET_PATH = os.path.join(PROJECT_PATH, 'luigi_targets')
Task.TARGET_DIR = TARGET_PATH
Task.is_cloud_target = True         # Change here to save targets locally.
Task.version = luigi.Parameter()
Task.resources = {'cpu': 1}


# App parameters: reading from Settings
#-----------------------------------------
from pycarol.apps import Apps
_settings = Apps(Carol()).get_settings()
now_str = datetime.now().isoformat()
connector_name = _settings.get('input_connector')
staging_name = _settings.get('input_staging')
model_l2regularization = _settings.get('model_l2regularization')
model_epochs = _settings.get('model_epochs')
deploy_app = _settings.get('deploy_app')


# Handling failure and conclusion events
#-----------------------------------------
@Task.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    logger.error(f'Error msg: {exception} ---- Error: Task {task},')
    traceback_str = ''.join(traceback.format_tb(exception.__traceback__))
    logger.error(traceback_str)

@Task.event_handler(luigi.Event.PROCESSING_TIME)
def print_execution_time(self, processing_time):
    logger.debug(f'### PROCESSING TIME {processing_time}s. Output saved at {self.output().path}')


# Making parameters available for tasks through the params dict.
#-----------------------------------------
params = dict(
    version=os.environ.get('CAROLAPPVERSION', 'dev'),

    datetime = now_str,
    staging_name = staging_name,
    connector_name = connector_name,
    model_epochs = model_epochs,
    model_l2regularization = model_l2regularization,
    deploy_app = deploy_app
)
