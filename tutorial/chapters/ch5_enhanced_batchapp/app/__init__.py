from pythonjsonlogger import jsonlogger
import os


class JsonFormatter(jsonlogger.JsonFormatter, object):
    def __init__(self, *args, **kwargs):

        super(JsonFormatter, self).__init__(*args, **kwargs)

    def process_log_record(self, log_record):
        log_record['severity'] = log_record['levelname']
        log_record['carol_task_id'] = os.environ.get('LONGTASKID', "NO TASK ID SET.")

        return log_record