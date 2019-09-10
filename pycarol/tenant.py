from .query import Query
import itertools
from .data_models import DataModel
from .staging import Staging

class Tenant:
    def __init__(self, carol):
        self.carol = carol

    def get_tenant_by_domain(self, domain):
        """
        Get tenant information.

        :param domain: `str`
            Tenant name
        :return:
            dict with the information about the tenant.
        """
        return self.carol.call_api('v2/tenants/domain/{}'.format(domain), auth=False, status_forcelist=[], retries=0)



    def check_exports(self, staging=True, data_model=True):
        """
        Find all data models or stating tables that have export active.

        :param staging: `bool`, default `True`
            Get status for exports in staging.
        :param data_model: `bool`, default `True`
            Get status for exports in data models.
        :return: `tuple`
            staging stats, data model stats
        """

        staging_results = None
        dm_results = None
        if staging:
            staging_results = Staging(self.carol)._get_staging_export_stats()

        if data_model:
            dm_results = DataModel(self.carol)._get_dm_export_stats()


        return staging_results, dm_results


