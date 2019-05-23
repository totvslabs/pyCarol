from ..luigi_extension import Task, WrapperTask
import luigi
from luigi import Parameter
import logging
import json
import os
import datetime
from ..apps import Apps
from ..carol import Carol
from ..staging import Staging

luigi.auto_namespace(scope=__name__)
logger = logging.getLogger(__name__)


class CarolAppConfig(luigi.Config):
    """ Task that contains configuration that might include parameters to load from Carol App

        1. Parameters are synchronized with Carol (back and forth)
        2. Documentation for app is generated from code and can be easily mantained/versioned.
        3. Single place to define all parameters / Less code on Run / facilitates logging/debugging/auditing

        app_carol: Dict with key:value parameters that came from Carol
        app_config: Dict with key:value parameters that are not from Carol
        app: Dict with key:value with all parameters

    """
    app_name = os.environ.get('CAROLAPPNAME')
    app_carol = None
    app = None

    @classmethod
    def initialization(cls):
        """ Define this function to do some logic on the parameters after they are extracted but before setting them
        on task. Parameters are saved on a variable named app.
        E.g.
            if cls.app['GROUP_ENTITY'] and cls.app['ENTITY'] != 'item':
                cls.app['ENTITY'] = 'short_mdmtaxid'
        """
        pass

    @classmethod
    def get_from_carol(cls, reset=False):
        """ Updates cls.app with values from Carol or Parameter object
        :return:
        """
        logger.debug('Getting Parameters data...')
        if cls.app is None:
            cls.app = {}

        for k, v in cls.get_params():
            if hasattr(v, 'carol') and v.carol:
                if cls.app_carol is None or reset:
                    login = Carol()
                    cls.app_carol = Apps(login)
                    cls.app_carol.get_settings(cls.app_name)
                    cls.app_carol = cls.app_carol.app_settings
                    for key, value in cls.app_carol.items():  # to avoid empty strings.
                        if value == '' or value is None:
                            cls.app_carol[key] = None
                        else:
                            try:
                                cls.app_carol[key] = json.loads(value)
                            except:
                                pass
                try:
                    if v.carol_name is not None:
                        logger.debug(f'{v.carol_name}: {cls.app_carol[v.carol_name]}')
                        carol_val = cls.app_carol[v.carol_name]
                    else:
                        carol_val = cls.app_carol[k]
                    # Convert Parameter type
                    if carol_val is not None:
                        if isinstance(v, luigi.DateParameter):
                            carol_val = datetime.datetime.strptime(carol_val, '%Y-%m-%d')  # TEMP - Carol should have a Date Parameter type
                    v._default = carol_val
                except KeyError as e:
                    logger.warning(f"Could not set up variable from Carol. Key = {str(e)}")
            cls.app.update({k: v})

        cls.initialization()
        return cls

    def to_carol(self):
        # TODO: Updates carol app with class parameters
        pass


class inherits_carol(object):
    """ Add CarolConfig parameters to a Task
    """

    def __init__(self, params_task):
        self.params_task = params_task

    def __call__(self, task_that_inherits):
        self.params_task.get_from_carol()
        for name, val in self.params_task.app.items():
                setattr(task_that_inherits, name, val)
        return task_that_inherits

# Create Luigi mappings


class StagingIngestion(Task):
    """ Task to execute Staging ingestion
    """

    tenant = Parameter()
    connector_name = Parameter()
    staging_name = Parameter()
    return_dask_graph = Parameter(significant=False, default=False)
    cols = Parameter(default=[])

    def easy_run(self, inputs):
        login = Carol()
        stag = Staging(login)
        logger.debug(f'Executing parquet query for staging {self.staging_name}')
        return stag.fetch_parquet(staging_name=self.staging_name, connector_name=self.connector_name,
                                  backend='pandas', return_dask_graph=self.return_dask_graph,
                                  columns=list(self.cols), merge_records=True)


class DataModelValidation(Task):
    """ Task to execute Data Model validation

    """
    tenant = Parameter(default='Unspecified')
    ignore_errors = Parameter(default=True)

    def easy_run(self, inputs):
        log = {'domain': self.tenant,
               'datamodel': self.dm.get_name()}

        success, log_dm = self.dm.validate(inputs[0], ignore_errors=self.ignore_errors)

        if not self.ignore_errors and not success:
            raise ValueError(log)
        log['log'] = log_dm
        return log
