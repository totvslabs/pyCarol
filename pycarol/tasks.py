import os


class Tasks:
    def __init__(self, carol):
        self.carol = carol

        self.task_id = None
        self.mdm_data = None
        self.mdm_user_id = None
        self.mdm_connector_id = None
        self.mdm_task_ready = None
        self.mdm_task_processing = None
        self.mdm_task_status = None
        self.mdm_task_owner = None
        self.mdm_task_progress = None
        self.mdm_process_after = None
        self.mdm_distribution_value = None
        self.mdm_task_priority = None
        self.mdm_number_of_steps = None
        self.mdm_number_of_steps_executed = None
        self.mdm_entity_type = None
        self.mdm_created = None
        self.mdm_last_updated = None
        self.mdm_tenant_id = None

    def _set_task_by_json(self, json_task):
        self.task_id = json_task['mdmId']
        self.mdm_user_id = json_task['mdmUserId']
        self.mdm_connector_id = json_task['mdmConnectorId']
        self.mdm_task_ready = json_task['mdmTaskReady']
        self.mdm_task_processing = json_task['mdmTaskProcessing']
        self.mdm_task_status = json_task['mdmTaskStatus']
        self.mdm_task_owner = json_task['mdmTaskOwner']
        self.mdm_task_progress = json_task['mdmTaskProgress']
        self.mdm_process_after = json_task['mdmProcessAfter']
        self.mdm_distribution_value = json_task['mdmDistributionValue']
        self.mdm_task_priority = json_task.get('mdmTaskPreference')
        self.mdm_number_of_steps = json_task['mdmNumberOfSteps']
        self.mdm_number_of_steps_executed = json_task['mdmNumberOfStepsExecuted']
        self.mdm_entity_type = json_task['mdmEntityType']
        self.mdm_created = json_task['mdmCreated']
        self.mdm_last_updated = json_task['mdmLastUpdated']
        self.mdm_tenant_id = json_task['mdmTenantId']

        if 'mdmData' in json_task:
            self.mdm_data = json_task['mdmData']

    def create(self, task_type, task_group, data={}):
        """
        Create a new task
        :param task_type: type of task
        :param task_group: commonly is used tenandId
        :param data: data used in the task
        :return: Task
        """
        dataJson = {
            "mdmTaskType": task_type,
            "mdmTaskGroup": task_group,
            "mdmData": data,
        }

        json_task = self.carol.call_api('v1/tasks/new', data=dataJson)
        self._set_task_by_json(json_task)
        return self

    def current_task(self):
        task_id = os.environ['LONGTASKID']
        if task_id is None:
            print("Can only get current_task if being called by Carol as a batch app")
        self.get_task(task_id)

    def get_current_task_id(self):
        task_id = os.environ['LONGTASKID']
        return task_id

    def set_as_current_task(self):
        os.environ['LONGTASKID'] = self.task_id

    def get_task(self, task_id=None):
        """
        Get Task
        :param task_id: task id
        :return: Task
        """

        if task_id is None:
            task_id = self.task_id

        json_task = self.carol.call_api('v1/tasks/{}'.format(task_id))
        self._set_task_by_json(json_task)
        return self

    def trace(self, log_message, task_id=None):
        self.add_log(log_message, "TRACE", task_id)

    def debug(self, log_message, task_id=None):
        self.add_log(log_message, "DEBUG", task_id)

    def info(self, log_message, task_id=None):
        self.add_log(log_message, "INFO", task_id)

    def warn(self, log_message, task_id=None):
        self.add_log(log_message, "WARN", task_id)

    def error(self, log_message, task_id=None):
        self.add_log(log_message, "ERROR", task_id)

    def add_log(self, log_message, log_level="INFO", task_id=None):
        """
        Add a log
        :param log_message: commonly used tenandId
        :param log_level: options: ERROR, WARN, INFO, DEBUG, TRACE
        :param task_id: it's not necessary if self.mdm_id is defined or if we are running from Carol as a batch app
        :return: boolean
        """

        if task_id is None:
            task_id = self.task_id
        if task_id is None:
            task_id = self.get_current_task_id()

        log = [{
            "mdmTaskId": task_id,
            "mdmLogMessage": log_message,
            "mdmLogLevel": log_level.upper()
        }]
        return self.add_logs(log)

    def add_logs(self, logs, task_id=None):
        """
        Add more than one log
        :param logs: list of logs objects [{"task_id":"", "log_message": "", "log_level": ""}]
        :param task_id: it's not necessary if self.mdm_id is defined
        :return: Task
        """

        if task_id is None:
            task_id = self.task_id

        resp = self.carol.call_api('v1/tasks/{}/logs'.format(task_id), data=logs)
        if resp['success']:
            return True
        else:
            return False

    def get_logs(self, task_id=None):
        """
        Get all logs
        :param task_id: it's not necessary if self.mdm_id is defined
        :return: list of logs
        """

        if task_id is None:
            task_id = self.task_id

        resp = self.carol.call_api('v1/tasks/{}/logs'.format(task_id))
        return resp

    def set_progress(self, progress, progress_data=None, task_id=None):
        """
        Set Task Progress
        :param progress: Number relative to progress
        :param progress_data: Json to storage as mdmTaskprogress_data
        :param task_id: it's not necessary if self.mdm_id is defined
        :return: Task
        """

        if progress_data is None:
            progress_data = {}
        else:
            assert isinstance(progress_data, dict)

        if task_id is None:
            task_id = self.task_id

        resp = self.carol.call_api('v1/tasks/{}/progress/{}'.format(task_id, progress), data=progress_data)
        return resp

    def cancel(self, task_id=None, force=False):
        """
        Cancel the task
        :param task_id: it's not necessary if self.mdm_id is defined
        :param force: Force cancel
        :return: boolean
        """

        if task_id is None:
            task_id = self.task_id

        querystring = {"force": force}

        resp = self.carol.call_api('v1/tasks/{}/cancel'.format(task_id), method="POST",params=querystring )
        if resp['success']:
            return True
        else:
            return False


## Missing Implements
# /api/v1/tasks/{id}/reprocess - Reprocess by Id
# /api/v1/tasks/{id}/sync - Process Task Synchronously by Id
# /api/v1/tasks/scheduled - Create Scheduled Task
# /api/v1/tasks/{id}/schedule - Schedule Similar Task by Id
# /api/v1/tasks/scheduled/{id} - Get Scheduled by Id
# /api/v1/tasks/scheduled/{id}/delete - Delete Scheduled Task by Id
# /api/v1/tasks/scheduled/{id}/pause - Cancel Scheduled Task by Id
# /api/v1/tasks/scheduled/{id}/play - Play Scheduled Task by Id
