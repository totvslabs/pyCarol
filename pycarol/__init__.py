""" PyCarol - Connecting Carol to Python

"""

import os
import tempfile

__version__ = '2.34.0'

__TEMP_STORAGE__ = os.path.join(tempfile.gettempdir(), 'carolina/cache')

__CONNECTOR_PYCAROL__ = 'f9953f6645f449baaccd16ab462f9b64'

_CAROL_METADATA_STAGING = ['mdmCounterForEntity', 'mdmId', 'mdmLastUpdated',
                           'mdmConnectorId', 'mdmDeleted']

_CAROL_METADATA_GOLDEN = ['mdmApplicationIdMasterRecordId', 'mdmCounterForEntity', 'mdmCreated',  'mdmCrosswalk',
                          'mdmDeleted', 'mdmEntityType',  'mdmId',  'mdmLastUpdated', 'mdmSourceEntityNames',
                          'mdmStagingRecord', 'mdmTenantId']

_NEEDED_FOR_MERGE = ['mdmCounterForEntity', 'mdmId', 'mdmDeleted']

from .carol import Carol
from .staging import Staging
from .connectors import Connectors
from .query import Query
from .storage import Storage
from .carolina import Carolina
from .tasks import Tasks
from .data_models import DataModel, DataModelView
from .logger import CarolHandler
from .auth.PwdAuth import PwdAuth
from .auth.ApiKeyAuth import ApiKeyAuth
from .auth.PwdKeyAuth import PwdKeyAuth
from .cds import CDSGolden, CDSStaging
from .apps import Apps
