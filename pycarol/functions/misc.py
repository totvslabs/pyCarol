import logging
from collections import defaultdict
import random
import time
from pycarol import (
    Carol, ApiKeyAuth, PwdAuth, Tasks, Staging, Connectors, CDSStaging, Subscription, DataModel, Apps
)

def track_tasks(carol, task_list, retry_count=3, logger=None, callback=None, polling_delay=5):
    """Track a list of taks from carol, waiting for errors/completeness. 

    Args:
        carol (pycarol.Carol): pycarol.Carol instance
        task_list (list): List of tasks in Carol
        retry_count (int, optional): Number of times to restart a failed task. Defaults to 3.
        logger (logging.logger, optional): logger to log information. Defaults to None.
        callback (calable, optional): This function will be called every time task status are fetched from carol. 
            A dictionary with task status will be passed to the function. Defaults to None.
        polling_delay (int, optional): Time in seconds to pull task status from Carol

    Returns:
        [dict, bool]: dict with status of each task and booling if any task failed more than retry_count times.
    """
    
    if logger is None:
        logger = logging.getLogger(carol.domain)

    retry_tasks = defaultdict(int)
    n_task = len(task_list)
    max_retries = set()
    carol_task = Tasks(carol)
    while True:
        task_status = defaultdict(list)
        for task in task_list:
            status = carol_task.get_task(task).task_status
            task_status[status].append(task)
        for task in task_status['FAILED'] + task_status['CANCELED']:
            logger.warning(f'Something went wrong while processing: {task}')
            retry_tasks[task] += 1
            if retry_tasks[task] > retry_count:
                max_retries.update([task])
                logger.error(
                    f'Task: {task} failed {retry_count} times. will not restart')
                continue

            logger.info(f'Retry task: {task}')
            carol_task.reprocess(task)

        if len(task_status['COMPLETED']) == n_task:
            logger.debug(f'All task finished')
            return task_status, False

        elif len(max_retries) + len(task_status['COMPLETED']) == n_task:
            logger.warning(f'There are {len(max_retries)} failed tasks.')
            return task_status, True
        else:
            time.sleep(round(polling_delay + random.random(), 2))
            logger.debug('Waiting for tasks')
        if callable(callback):
            callback(task_status)


def pause_dm_mappings(carol, dm_list, connector_name=None, connector_id=None, do_not_pause_staging_list=None):
    """Pause mappings from a connetor based on alist of Datamodels.

    Args:
        carol (pycal.Carol): Carol instance
        dm_list (list): list of Datamodels to pause
        connector_name (str): connector name
        connector_name (str): connector id
        do_not_pause_staging_list (list, optional): List of stagings to do not pause. Defaults to None.
    """

    do_not_pause_staging_list = do_not_pause_staging_list if do_not_pause_staging_list else ['']
    conn = Connectors(carol)
    mappings = conn.get_dm_mappings(connector_name=connector_name, connector_id=connector_id )
    mappings = mappings = [i['mdmId'] for i in mappings if
                           (i['mdmRunningState'] == 'RUNNING') and
                           (i['mdmMasterEntityName'] in dm_list) and
                           (i['mdmStagingType'] not in do_not_pause_staging_list)
                           ]

    _ = conn.pause_mapping(
        connector_name=connector_name, entity_mapping_id=mappings, 
        connector_id=connector_id)