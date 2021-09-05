import logging
from collections import defaultdict
import random
import time
from itertools import chain

from pycarol import (
    Carol, ApiKeyAuth, PwdAuth, Tasks, Staging, Connectors, CDSStaging, Subscription, DataModel, Apps, CDSGolden
)
from pycarol.query import delete_golden

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

    Usage:

    .. code:: python

        from pycarol import Carol
        from pycarol.functions import track_tasks
        carol = Carol()
        def callback(task_list):
            print(task_list)
        track_tasks(carol=carol, task_list=['task_id_1', 'task_id_2'], callback=callback)

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
    """Pause mappings from a connetor based on a list of Datamodels.

    Args:

        carol (pycal.Carol): Carol instance
        dm_list (list): list of Datamodels to pause
        connector_name (str): connector name
        connector_name (str): connector id
        do_not_pause_staging_list (list, optional): List of stagings to do not pause. Defaults to None.

    Usage:

    .. code:: python

        from pycarol import Carol
        from pycarol.functions import pause_dm_mappings
        carol = Carol()
        pause_dm_mappings(carol, connector_name='connector_name', dm_list=['dm1','dm2'])

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

def check_mapping(login, staging_name, connector_name=None, connector_id=None):
    """Check if a staging is mapped to a datamodel and return the mapping.

    Args:

        login (pycaol.Carol): Carol instance
        staging_name ([type]): staging name
        connector_name ([type]): connetor name
        connector_id ([type]): connetor id

    Usage:

    .. code:: python

        from pycarol import Carol
        from pycarol.functions import check_mapping
        carol = Carol()
        check_mapping(carol, staging_name='staging_name', connector_name='connector_name')

    Returns:

        list: list of mappings or None

    """
    conn = Connectors(login)

    if connector_id is None and connector_name is None:
        raise ValueError('Either connector_id or connector_name must be set.')
    connector_id = connector_id if connector_id else conn.get_by_name(connector_name)['mdmId']
    resp = conn.get_entity_mappings(
        connector_name=connector_name, staging_name=staging_name, connector_id=connector_id, 
        errors='ignore'
        )

    if isinstance(resp, dict):
        if resp['errorCode'] == 404 and 'Entity mapping not found' in resp['errorMessage']:
            return None
        else:
            raise ValueError(f'Error checking mapping {resp}')

    return resp

def resume_process(carol, staging_name, connector_name=None, connector_id=None, logger=None, delay=1):
    """Resume staging process (mappings or ETLs)

    Args:

        carol (pycarol.Carol): Carol instance
        staging_name (str): staging name
        connector_name (str): connector name
        connector_id (str): connetor id
        logger (logging.logger, optional): logger. Defaults to None.
        delay (int, optional): time to wait after resume processing. Defaults to 1.


    Usage:

    .. code:: python

        from pycarol import Carol
        from pycarol.functions import resume_process
        carol = Carol()
        resume_process(carol, staging_name='staging_name', connector_name='connector_name')

    Returns:

        list: list of mappings paused.
        
    """
    if logger is None:
        logger = logging.getLogger(carol.domain)

    conn = Connectors(carol)

    if connector_id is None and connector_name is None:
        raise ValueError('Either connector_id or connector_name must be set.')
    connector_id = connector_id if connector_id else conn.get_by_name(connector_name)['mdmId']
    # TODO Review this once we have mapping and ETLs in the same staging.
    # Play ETLs if any.
    resp = conn.play_etl(connector_id=connector_id, staging_name=staging_name, connector_name=connector_name)
    if not resp['success']:
        logger.error(f'Problem starting ETL {connector_name}/{staging_name}\n {resp}')
        raise ValueError(f'Problem starting ETL {connector_name}/{staging_name}\n {resp}')

    # Play mapping if any.
    mappings_list = check_mapping(carol, connector_name=connector_name, staging_name=staging_name, connector_id=connector_id)
    if mappings_list is not None:
        for mappings_ in mappings_list:
            conn.play_mapping(connector_name=connector_name, connector_id=connector_id, 
            entity_mapping_id=mappings_['mdmId'], process_cds=False, )

    # wait for mapping effect.
    time.sleep(delay)
    return mappings_list


def pause_etls(carol, etl_list, connector_name=None, connector_id=None, logger=None):
    """Pause ETLs from a connetor based on a list of ETLs.

    Args:

        carol (pycarol.Carol): Carol instance
        etl_list (list): ETLs to pause
        connector_name (str, optional): Connector Name. Defaults to None.
        connector_id (str, optional): Connector ID. Defaults to None.
        logger (logging.logger, optional): Logger. Defaults to None.

    Usage:

    .. code:: python

        from pycarol import Carol
        from pycarol.functions import resume_process
        carol = Carol()
        etl_list = ['staging1', 'staging2', 'staging3']
        pause_etls(carol, connector_name='rui', etl_list=etl_list,)

    Returns:

        list: list of paused ETLs

    """
    if logger is None:
        logger = logging.getLogger(carol.domain)

    conn = Connectors(carol)
    if connector_id is None and connector_name is None:
        raise ValueError('Either connector_id or connector_name must be set.')
    connector_id = connector_id if connector_id else conn.get_by_name(connector_name)['mdmId']

    r = {}
    for staging_name in etl_list:
        logger.debug(f'Pausing {staging_name} ETLs')
        r[staging_name] = conn.pause_etl(connector_name=connector_name, staging_name=staging_name)

    if not all(i['success'] for _, i in r.items()):
        logger.error(f'Some ETLs were not paused. {r}')
        raise ValueError(f'Some ETLs were not paused. {r}')

    return r

def delele_all_golden_data(carol, dm_name):
    """Delete golden files from a datamodel in all storages.

    Args:
        carol (pycarol.Carol): Carol instance
        dm_name (str): Data Model name

    Returns:
        list: list of tasks created
    """
    
    cds_CDSGolden = CDSGolden(carol)
    t = []
    dm_id = DataModel(carol).get_by_name(dm_name)['mdmId']
    task = cds_CDSGolden.delete_rejected(dm_id=dm_id, )

    t += [task]
    task = cds_CDSGolden.delete(dm_id=dm_id, )
    delete_golden(carol, dm_name)
    t += [task['taskId'], ]
    return t

def par_delete_golden(carol, dm_list, n_jobs=5):
    """
    Deletes golden files from a list of datamodels in parallel.

    Args:
        carol (pycarol.Carol): Carol instance
        dm_list (list): List of datamodels
        n_jobs (int, optional): Number of parallel jobs. Defaults to 5.

    Returns:
        list: list of tasks created
    """
    from joblib import Parallel, delayed
    tasks = Parallel(n_jobs=n_jobs)(delayed(delele_all_golden_data)(carol, i)
                                    for i in dm_list)
    return list(chain(*tasks))


def delete_staging_data(carol, staging_name, connector_name):
    """Delete a staging.

    Args:
        carol (pycarol.Carol): Login instance
        staging_name (str): Staging name
        connector_name (str): Connector name
    """ 

    t = []
    cds_ = CDSStaging(carol)
    task = cds_.delete(staging_name=staging_name,
                        connector_name=connector_name)
    t += [task['taskId'], ]
    return t

def par_delete_staging(carol, staging_list, connector_name, n_jobs=5):
    """
    Deletes staging files from a list of datamodels in parallel.

    Args:
        carol (pycarol.Carol): Login instance
        staging_list (list): List of datamodels
        connector_name (str): Connector name
        n_jobs (int, optional): Number of parallel jobs. Defaults to 5.

    Returns:
        list: list of tasks created
    """
    from joblib import Parallel, delayed
    tasks = Parallel(n_jobs=n_jobs)(delayed(delete_staging_data)(carol, i, connector_name)
                                    for i in staging_list)
    return list(chain(*tasks))
