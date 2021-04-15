from .carol import Carol
from .data_models import DataModel
from .staging import Staging
from .apps import Apps
from .cds import CDSGolden, CDSStaging
from .connectors import Connectors
from .query import Query
from .storage import Storage
from .carolina import Carolina
from .tasks import Tasks
from .subscription import Subscription
from functools import partial

from .subscription import Subscription


CAROL_KNOWN_MODULES = {
    'datamodel': DataModel,
    'staging': Staging,
    'apps': Apps,
    'cds_golden': CDSGolden,
    'cds_staging': CDSStaging,
    'query': Query,
    # 'storage': Storage,  #Had to remove these two because they have google client that is unpickleable. This breaks if trying to use in any multiprocessing lib.
    # 'carolina': Carolina,
    'task': Tasks,
    'subscription': Subscription,
    'connector': Connectors,
}


class CarolAPI(Carol):
    """Encapsulates some pycarol modules

    Args:

        domain: `str`. default `None`.
            Tenant name. e.x., domain.carol.ai
        app_name: `str`. default `None`.
            Carol app name.
        auth: `PwdAuth` or `ApiKeyAuth`.
            object Auth Carol object to handle authentication
        connector_id: `str` , default `__CONNECTOR_PYCAROL__`.
            Connector Id
        port: `int` , default 443.
            Port to be used (when running locally it could change)
        verbose: `bool` , default `False`.
            If True will print the header, method and URL of each API call.
        organization: `str` , default `None`.
            Organization domain.
        environment: `str`, default `carol.ai`,
            Which Carol's environment to use. There are three possible values today.

                1. 'carol.ai' for the production environment
                2. 'karol.ai' for the explore environment
                3. 'qarol.ai' for the QA environment

        host: `str` default `None`
            This will overwrite the host used. Today the host is:

                1. if organization is None, host={domain}.{environment}
                2. else host={organization}.{environment}

            See Carol._set_host.

         user:  `str` default `None`
            User
         password: `str` default `None`
            User passowrd
         api_key:  `str` default `None`
            Carol's Api Key

    OBS:
        In case all parameters are `None`, pycarol will try yo find their values in the environment variables.
        The values are:

             1. `CAROLTENANT` for domain
             2. `CAROLAPPNAME` for app_name
             3. `CAROLAPPOAUTH` for auth
             4. `CAROLORGANIZATION` for organization
             5. `CAROLCONNECTORID` for connector_id
             6. `CAROL_DOMAIN` for environment
             7. `CAROLUSER` for carol user email
             8. `CAROLPWD` for user password.

    Usage:

    .. code:: python

            from pycarol import CarolAPI
            carol = CarolAPI()
            # saving a file using storage
            carol.storage.save(name='myfile.csv', obj='/local/file/.csv',  format='file')
            #fetch data from staging
            df = carol.staging.fetch_parquet(staging_name=staging,  
                                        connector_name=connector_name,)
            # show all the available modules. 
            carol._all_modules

    If one wants to use a module not in "carol._all_modules" it is possible to add new modules using:

    .. code:: python

            from pycarol import CarolAPI
            carol = CarolAPI()
            carol.add_module('new_module', 'ANewModule')

    Note that this module will receive pycarol.Carol as the first argument. If the module require extra arguments,
    use:

    .. code:: python

        from pycarol import CarolAPI
        carol = CarolAPI()
        carol.add_module('new_module', 'ANewModule', allow_args=True)

        carol.new_module(extra_arg, new_kwarg='foo').do_something()

    """

    def __init__(self, domain=None, app_name=None, auth=None, connector_id=None, port=443, verbose=False,
                 organization=None, environment=None, host=None, user=None, password=None, api_key=None):

        super(CarolAPI, self).__init__(domain=domain, app_name=app_name, auth=auth, connector_id=connector_id, port=port, verbose=verbose,
                                       organization=organization, environment=environment, host=host, user=user, password=password, api_key=api_key)

        self._all_modules = set()
        self._create_context()

    def _create_context(self):

        for module_name, module in CAROL_KNOWN_MODULES.items():

            if module is Query:
                allow_args = True
            else:
                allow_args = False

            self.add_module(module_name, module, allow_args=allow_args)

    def add_module(self, module_name, module, allow_args=False):
        """Adds module to context
        Args:
            module_name (str): attribute to be created
            module (pyCarol modude): the module will be inicialized with the pycarol.Carol instance.
            allow_args (bool): use functools.partial to allow passing arguments when calling the module. 
        """
        if allow_args:
            module = partial(module, self)
            setattr(self, module_name, module)
        else:
            setattr(self, module_name, module(self))
        self._all_modules.update([module_name])
