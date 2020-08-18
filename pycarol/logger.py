"""
Logger Handler integrated with Carol.

"""

from . import Tasks, Carol
import os
import logging
import sys
import re

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
    """
    Carol logger handler.

    This class can be used to log information in long tasks in Carol.

    Args:

        carol: Carol object
            Carol object.

    Usage:

    .. code:: python

        from pycarol import Carol, CarolHandler
        import logging
        logger = logging.getLogger(__name__)
        carol = CarolHandler(Carol())
        carol.setLevel(logging.INFO)
        logger.addHandler(carol)

        logger.debug('This is a debug message') #This will not be logged in Carol. Level is set to INFO
        logger.info('This is an info message')
        logger.warning('This is a warning message')
        logger.error('This is an error message')
        logger.critical('This is a critical message')

        #These methods will use the current long task id provided by Carol when running your application.
        #For local environments you need to set that manually first on the beginning of your code:

        import os
        os.environ['LONGTASKID'] = TASK_ID

    If no TASK ID is passed it works as a Console Handler.
    """

    def __init__(self, carol=None):
        """

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
        self._first_pending = True

    def _log_carol(self, record):
        msg = self.format(record)
        log_level = _carol_levels.get(record.levelname)
        self._task.add_log(msg, log_level=log_level)

    def emit(self, record):
        """
         Log the message.

        Args:
            record: `str`
                Message to log.

        Returns: None

        """

        if (self.task_id is None) or (self._use_console):
            super().emit(record)
        else:
            self._log_carol(record)

