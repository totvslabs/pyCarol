"""PyCarol - Connecting Carol to Python."""
import os
import tempfile

__version__ = "2.54.14"

__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(), "carolina/cache")

__CONNECTOR_PYCAROL__ = "f9953f6645f449baaccd16ab462f9b64"

_CAROL_METADATA_STAGING = [
    "mdmCounterForEntity",
    "mdmId",
    "mdmLastUpdated",
    "mdmCreated",
    "mdmConnectorId",
    "mdmDeleted",
]

_CAROL_METADATA_GOLDEN = [
    "mdmCounterForEntity",
    "mdmStagingCounter",
    "mdmId",
    "mdmCreated",
    "mdmLastUpdated",
    "mdmTenantId",
    "mdmEntityType",
    "mdmSourceEntityNames",
    "mdmCrosswalk",
    "mdmStagingRecord",
    "mdmApplicationIdMasterRecordId",
    "mdmPreviousIds",
    "mdmDeleted",
]

_CAROL_METADATA_UNTIE_GOLDEN = "mdmStagingCounter"
_CAROL_METADATA_UNTIE_STAGING = "mdmCounterForEntity"


_NEEDED_FOR_MERGE = ["mdmCounterForEntity", "mdmId", "mdmDeleted"]

_REJECTED_DM_COLS = [
    "mdmConnectorId",
    "mdmCreated",
    "mdmCreatedUser",
    "mdmCrosswalk",
    "mdmDetailFieldValues",
    "mdmEntityTemplateId",
    "mdmEntityType",
    "mdmErrors",
    "mdmId",
    "mdmLastUpdated",
    "mdmMasterFieldAndValues",
    "mdmStagingApplicationId",
    "mdmStagingEntityName",
    "mdmStagingRecord",
    "mdmTenantId",
    "mdmUpdatedUser",
]


from . import bigquery  # noqa
from .apps import Apps  # noqa
from .auth.PwdAuth import PwdAuth  # noqa
from .auth.ApiKeyAuth import ApiKeyAuth  # noqa
from .auth.PwdKeyAuth import PwdKeyAuth  # noqa
from .auth.PwdFluig import PwdFluig  # noqa
from .bigquery import BQ, BQStorage  # noqa
from .carol import Carol  # noqa
from .carol_api import CarolAPI  # noqa
from .carolina import Carolina  # noqa
from .cds import CDSGolden, CDSStaging  # noqa
from .connectors import Connectors  # noqa
from .data_models import DataModel  # noqa
from .logger import CarolHandler  # noqa
from .query import Query  # noqa
from .staging import Staging  # noqa
from .storage import Storage  # noqa
from .subscription import Subscription  # noqa
from .tasks import Tasks  # noqa
