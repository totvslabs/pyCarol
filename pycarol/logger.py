from . import Tasks
import os
import logging



_carol_levels = dict(
    INFO="INFO",
    WARNING="WARN",
    ERROR="ERROR",
    CRITICAL="ERROR"
)

class CarolHandler(logging.StreamHandler):

    def __init__(self, carol, level=logging.INFO):
        """

        Carol logger handler.

        This class can be used to log informatio in long tasks in Carol.

        :param carol: Carol object
            Carol object.
        :param get_times: `int`, default `20`
            Log level. By default, in python, there are 5 standard levels indicating the severity of events.
            20 is lvel INFO.

        """
        super().__init__(level)
        self.carol = carol
        self._task = Tasks(self.carol)
        self.task_id = os.getenv('LONGTASKID', None)
        self._task.task_id = self.task_id



    def _log_carol(self, record):
        msg = self.format(record)
        log_level = _carol_levels.get(record.levelname)
        self._task.add_log(msg, log_level=log_level)



    def emit(self, record):
        if self.task_id is None:
            super().__init__(record)
        else:
            self._log_carol(record)
