from pycarol.pipeline import Task
import luigi

import logging
import traceback

logger = logging.getLogger(__name__)

Task.is_cloud_target = False  # With true Carol env variable will be used, and all tasks outputs will be saved in CDS.
Task.TARGET_DIR = 'my_app_target_folder'  # this is used when `is_cloud_target==False`. It will create a fodler called `my_app_target_folder` in the current directory.
# I can add "global" parameters that will be present in all tasks as well:
Task.version = luigi.Parameter()


# Some callbacks that can be called after a task runs. Check Luigi event handlers for all the possible callbacks.
@Task.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any Task
    """

    logger.error(f'Error msg: {exception} ---- Error: Task {task},')
    # print the stack trace
    traceback_str = ''.join(traceback.format_tb(exception.__traceback__))
    logger.error(traceback_str)


@Task.event_handler(luigi.Event.PROCESSING_TIME)
def print_execution_time(self, processing_time):
    """
    This will print the execution time for each task and the path where the target was saved.
    Notice that self is the instace of that task. So you have access to all parameters for that task

    """
    logger.debug(f'### PROCESSING TIME {processing_time}s. Output saved at {self.output().path}')


# task commons params to run the pipeline.
params = {
    "version": '1.0.0',
    "param_1": "1",
    "param_2": "2",
    "param_3": "3",
    "param_4": "4",
    "param_5": "5",
}
