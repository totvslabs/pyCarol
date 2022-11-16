from functools import partial
import typing as T

from .apps import Apps
from .carol import Carol
from .cds import CDSGolden, CDSStaging
from .connectors import Connectors
from .data_models import DataModel
from .query import Query
from .staging import Staging
from .subscription import Subscription
from .tasks import Tasks


class CarolAPI(Carol):

    """Encapsulates some pycarol modules.

    Args:
        This class use the same arguments as pycarol.Carol.

    Usage:

    .. code:: python

            from pycarol import CarolAPI
            carol = CarolAPI()
            # saving a file using storage
            carol.storage.save(name='myfile.csv', obj='file.csv',  format='file')
            df = carol.staging.fetch_parquet(
                staging_name=staging,
                connector_name=connector_name
            )

    If one wants to use a module not in  it is possible to add new
    modules using:

    .. code:: python

            from pycarol import CarolAPI
            carol = CarolAPI()
            carol.add_module('new_module', 'ANewModule')

    Note that this module will receive pycarol.Carol as the first argument. If the
    module require extra arguments,
    use:

    .. code:: python

        from pycarol import CarolAPI
        carol = CarolAPI()
        carol.add_module('new_module', 'ANewModule', allow_args=True)

        carol.new_module(extra_arg, new_kwarg='foo').do_something()
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ):

        super().__init__(
            *args,
            **kwargs
        )

        self._all_modules = set()

        self.apps = Apps(self)
        self.connector = Connectors(self)
        self.cds_golden = CDSGolden(self)
        self.cds_staging = CDSStaging(self)
        self.datamodel = DataModel(self)
        self.query = partial(Query, self)
        self.staging = Staging(self)
        self.subscription = Subscription(self)
        self.task = Tasks(self)
        # Had to remove these two because they have google client that is unpickleable.
        # This breaks if trying to use in any multiprocessing lib.
        # 'storage': Storage,
        # 'carolina': Carolina,

    def add_module(
        self, module_name: str, module: T.Callable, allow_args: bool = False
    ) -> None:
        """Add module to context.

        @DEPRECATED. This function will be removed in future releases.

        Args:
            module_name: attribute to be created
            module: the module will be inicialized with the pycarol.Carol instance.
            allow_args: use functools.partial to allow passing arguments when calling
                the module.
        """
        if allow_args is True:
            setattr(self, module_name, partial(module, self))
        else:
            setattr(self, module_name, module(self))
