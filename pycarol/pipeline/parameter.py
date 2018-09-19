import luigi
import json
import logging
from ..apps import Apps


logger = logging.getLogger(__name__)


class set_parameters(object):
    """ Organize all parameters synchronization with Carol
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


class SettingsDefinition(luigi.Task):
    api_token = None
    app_name = None
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
    def get_app_params(cls):
        """ Updates cls.app with values from Carol or default
        :return:
        """
        if cls.app is None:
            cls.app = {}
        app_carol = None
        for k, v in cls.get_params():
            if v.carol:
                if app_carol is None:
                    if cls.api_token is not None:
                        app_carol = Apps(cls.api_token)
                        app_carol.get_settings(cls.app_name)
                        app_carol = app_carol.app_settings
                        for key, value in app_carol.items():  # to avoid empty strings.
                            if value == '' or value is None:
                                app_carol[key] = None
                            else:
                                try:
                                    app_carol[key] = json.loads(value)
                                except json.JSONDecodeError:
                                    pass
                    else:
                        app_carol = cls.app  # If no login token specified, assume it is already declared on app
                # TODO Add try catch to log error an not throw exception when a value is not found on app
                try:
                    if v.carol_name is not None:
                        v = app_carol[v.carol_name]
                    else:
                        v = app_carol[k]
                except KeyError as e:
                    logger.error(f"Could not set up variable from Carol. Key = {str(e)}")

            cls.app.update({k: v})

        # TODO warn if parameters defined in class are not set in cls.app
        cls.initialization()
        return cls


class Parameter(luigi.Parameter):
    login_token = None

    def __init__(self, default=None, carol=False, carol_name=None, **kwargs):
        super().__init__(default=default, **kwargs)
        self.carol_name = carol_name
        self.carol = carol
        if carol:
            self.default = None
