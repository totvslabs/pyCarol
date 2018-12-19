from luigi_extension import Task, WrapperTask
import luigi
from luigi import Parameter, DictParameter
import logging
import json
import os
from ..apps import Apps
from ..staging import Staging
from ..carol import Carol

logger = logging.getLogger(__name__)


class Parameter(Parameter):
    """ Extension of Parameter to include Carol information
    """

    def __init__(self, default=None, carol=False, carol_name=None, **kwargs):
        super().__init__(default=default, **kwargs)
        self.carol_name = carol_name
        self.carol = carol
        if carol:
            self.default = None


class DictParameter(DictParameter):
    """ Extension of DictParameter to include Carol information
    """

    def __init__(self, default=None, carol=False, carol_name=None, **kwargs):
        super().__init__(default=default, **kwargs)
        self.carol_name = carol_name
        self.carol = carol
        if carol:
            self.default = None


class SettingsDefinition(luigi.Task):
    """ Task that contains all parameters necessary for the pipeline execution
    """
    app_name = os.environ.get('CAROLAPPNAME')
    app_carol = None
    app = None

    @classmethod
    def get_params(cls):
        """
        Returns all of the Parameters for this Task, including luigi_extension's new Parameter class.
        """
        # We want to do this here and not at class instantiation, or else there is no room to extend classes dynamically
        params = []
        for param_name in dir(cls):
            param_obj = getattr(cls, param_name)
            if not (isinstance(param_obj, Parameter) or isinstance(param_obj, luigi.Parameter)):
                continue
            params.append((param_name, param_obj))

        # The order the parameters are created matters. See Parameter class
        params.sort(key=lambda t: t[1]._counter)
        return params

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
    def get_app_params(cls):
        """ Updates cls.app with values from Carol or default
        :return:
        """
        logger.debug('Getting Parameters data...')
        if cls.app is None:
            cls.app = {}

        for k, v in cls.get_params():
            if v.carol:
                if cls.app_carol is None:
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
                        v = cls.app_carol[v.carol_name]
                    else:
                        v = cls.app_carol[k]
                        logger.debug(f'{k}: {v}')
                except KeyError as e:
                    logger.warning(f"Could not set up variable from Carol. Key = {str(e)}")
            cls.app.update({k: v})

        # TODO warn if parameters defined in class are not set in cls.app

        cls.initialization()
        return cls


class set_parameters(object):
    """ Decorator = Organize all parameters synchronization with Carol

        1. Parameters are synchronized with Carol (back and forth)
        2. Documentation for app is generated from code and can be easily mantained/versioned.
        3. Single place to define all parameters / Less code on Run / facilitates logging/debugging/auditing

    """

    def __init__(self, params_task):
        self.params_task = params_task

    def __call__(self, task_that_inherits):
        self.params_task.get_app_params()
        for name, val in self.params_task.app.items():
            if isinstance(val, Parameter) or isinstance(val, luigi.Parameter):
                setattr(task_that_inherits, name, val)
            else:
                setattr(task_that_inherits, name, Parameter(default=val))
        return task_that_inherits


class DataModelValidation(Task):
    """ Task to execute Data Model validation

    """
    domain = Parameter(default='Unspecified')
    ignore_errors = Parameter(default=True)

    def easy_run(self, inputs):
        log = {'domain': self.domain,
               'datamodel': self.dm.get_name()}

        success, log_dm = self.dm.validate(inputs[0], ignore_errors=self.ignore_errors)

        if not self.ignore_errors and not success:
            raise ValueError(log)
        log['log'] = log_dm
        return log


# Create Luigi mappings

class StagingIngestion(Task):
    """ Task to execute Staging ingestion

    """

    domain = Parameter()
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

# Create tasks from Data Models information automatically


class Ingestion(WrapperTask):
    """ Generic task for ingestion

        This task will get from exec_config.get_ingestion_task which task should be executed.

        ingestion_params: dict with all necessary information for the ingestion. Must have at least one field named
        'mapping'. Each mapping will have specific requirements that must be placed on this parameter.
        Refer to the specific mapping documentation for more details.

        E.g.
        {'mapping':'carol_golden'} will execute mapping from Carol MDM Golden Records

    """
    domain = Parameter()
    dm_name = Parameter()

    ingestion_params = DictParameter()
    filter = DictParameter()

    def requires(self):
        from app import exec_config
        if self.ingestion_params is None:
            raise ValueError(f'Did not receive information for Ingestion. Received: {self.ingestion_params}')
        if 'mapping' not in self.ingestion_params:
            raise ValueError('Could not find mapping information for Ingestion task.')
        return self.clone(exec_config.get_ingestion_task(self.ingestion_params['mapping'], dm_name=self.dm_name),
                          **self.ingestion_params)
