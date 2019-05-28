from . import Tasks, Carol
import os
import logging
import sys

_carol_levels = dict(
    NOTSET="NOTSET",
    DEBUG='DEBUG',
    INFO="INFO",
    WARNING="WARN",
    WARN="WARN",
    ERROR="ERROR",
    CRITICAL="ERROR",
    FATAL="ERROR",
)


class CarolHandler(logging.StreamHandler):

    def __init__(self, carol=None):
        """

        Carol logger handler.

        This class can be used to log informatio in long tasks in Carol.

        :param carol: Carol object
            Carol object.

        """
        super().__init__(stream=sys.stdout)

        self._use_console = False
        if carol is None:
            domain = os.getenv('CAROLTENANT')
            app_name = os.getenv('CAROLAPPNAME')
            auth_token = os.getenv('CAROLAPPOAUTH')
            connector_id = os.getenv('CAROLCONNECTORID')
            if ((domain is not None)
                    and (app_name is not None)
                    and (auth_token is not None)
                    and (connector_id is not None)):
                carol = Carol()
                self._use_console = False

            else:
                self._use_console = True

        self.carol = carol
        self._task = Tasks(self.carol)
        self.task_id = os.getenv('LONGTASKID', None)
        self._task.task_id = self.task_id

    def _log_carol(self, record):
        msg = self.format(record)
        log_level = _carol_levels.get(record.levelname)
        self._task.add_log(msg, log_level=log_level)

    def emit(self, record):
        if (self.task_id is None) or (self._use_console):
            super().emit(record)
        else:
            self._log_carol(record)
