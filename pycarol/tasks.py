
class Tasks:
    def __init__(self, carol):
        self.carol = carol

    def _getTaskId(self, task):
        if type(task) is dict:
            return task['mdmId']
        else:
            return task

    def create(self, taskType, taskGroup, data={}):
        """
        Create a new task
        :param taskType: type of task
        :param taskGroup: commonly is used tenandId
        :param data: data used in the task
        :return: Task
        """

        dataJson = {
            "mdmTaskType": taskType,
            "mdmTaskGroup": taskGroup,
            "mdmData": data,
        }

        resp = self.carol.call_api('v1/tasks/new', data=dataJson)
        print (resp)
        return resp


    def getTask(self, task):
        """
        Get Task
        :param task: Task or task id
        :return: Task
        """

        taskId = self._getTaskId(task)

        resp = self.carol.call_api('v1/tasks/{}'.format(taskId))
        return resp


    def addLog(self, task, logMessage, logLevel="WARN"):
        """
        Add a log
        :param task: Task or task id
        :param logMessage: commonly used tenandId
        :param logLevel: options: ERROR, WARN, INFO, DEBUG, TRACE
        :return: boolean
        """

        taskId = self._getTaskId(task)

        log = [{
            "mdmTaskId": taskId,
            "mdmLogMessage": logMessage,
            "mdmLogLevel": logLevel.upper()
        }]
        return self.addLogs(taskId, log)


    def addLogs(self, task, logs=[]):
        """
        Add more than one log
        :param task: Task or task id
        :param logs: array of logs objects [{"taskId":"", "logMessage": "", "logLevel": ""}]
        :return: Task
        """

        taskId = self._getTaskId(task)

        resp = self.carol.call_api('v1/tasks/{}/logs'.format(taskId), data=logs)
        if resp['success']:
            return True
        else:
            return False


    def getLogs(self, task):
        """
        Get all logs
        :param task: Task or task id
        :return: list of logs
        """

        taskId = self._getTaskId(task)

        resp = self.carol.call_api('v1/tasks/{}/logs'.format(taskId))
        return resp


    def setProgress(self, task, progress, progressData={}):
        """
        Set Task Progress
        :param task: Task or task id
        :param progress: Number relative to progress
        :param progressData: Json to storage as mdmTaskProgressData
        :return: Task
        """

        taskId = self._getTaskId(task)

        resp = self.carol.call_api('v1/tasks/{}/progress/{}'.format(taskId, progress), data=progressData)
        return resp


    def cancel(self, task):
        """
        Cancel the task
        :param task: Task or task id
        :return: boolean
        """

        taskId = self._getTaskId(task)

        resp = self.carol.call_api('v1/tasks/{}/cancel'.format(taskId), method="POST")
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
