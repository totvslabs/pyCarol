from luigi_extension import Task, WrapperTask, inherit_list, Parameter
import luigi
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
    login_token = None

    def __init__(self, default=None, carol=False, carol_name=None, **kwargs):
        super().__init__(default=default, **kwargs)
        self.carol_name = carol_name
        self.carol = carol
        if carol:
            self.default = None


class SettingsDefinition(luigi.Task):
    """ Task that contains all parameters necessary for the pipeline execution
    """
    api_token = None
    app_name = None
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
        for k, v in cls.get_params():
            if v.carol:
                if cls.app_carol is None:
                    login = Carol()
                    app_carol = Apps(login)
                    app_carol.get_settings(os.get['CAROLAPPNAME'])
                    app_carol = app_carol.app_settings
                    for key, value in app_carol.items():  # to avoid empty strings.
                        if value == '' or value is None:
                            app_carol[key] = None
                        else:
                            try:
                                app_carol[key] = json.loads(value)
                            except:
                                pass
                try:
                    if v.carol_name is not None:
                        logger.debug(f'{v.carol_name}: {app_carol[v.carol_name]}')
                        v = app_carol[v.carol_name]
                    else:
                        v = app_carol[k]
                        logger.debug(f'{k}: {v}')
                except KeyError as e:
                    logger.warning(f"Could not set up variable from Carol. Key = {str(e)}")
                    logger.debug(app_carol)

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

    domain = Parameter()
    connector_name = Parameter()
    staging_name = Parameter()
    return_dask_graph = Parameter(significant=False, default=False)
    cols = Parameter(default=[])

    def easy_run(self, inputs):
        login = Carol()
        stag = Staging(login)
        return stag.fetch_parquet(staging_name=self.staging_name, connector_name=self.connector_name,
                                  backend='pandas', return_dask_graph=self.return_dask_graph,
                                  columns=list(self.cols), merge_records=True)

# Create tasks from Data Models information automatically


class Ingestion(WrapperTask):
    """
        ingestion_params: dict with all necessary information for the ingestion. Must have at least one field named
        'mapping'. Each mapping will have specific requirements that must be placed on this parameter. Refer to the
        mapping documentation for more details.

    """
    domain = Parameter()
    dm = Parameter()

    ingestion_params = Parameter()
    filter = Parameter()

    def requires(self):
        from app import exec_config
        params = self.ingestion_params[0][1]
        params = dict(params)
        if 'mapping' not in params:
            raise ValueError('Could not find mapping information for Ingestion task.')
        return self.clone(exec_config.get_ingestion_task(params['mapping'], dm_name=self.dm.get_name()), **params)
