from .utils.deprecation_msgs import _deprecation_msgs


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

        @DEPRECATED. This function was removed in pycarol 3.34

        Find all data models or stating tables that have export active.

        :param staging: `bool`, default `True`
            Get status for exports in staging.
        :param data_model: `bool`, default `True`
            Get status for exports in data models.
        :return: `tuple`
            staging stats, data model stats
        """

        _deprecation_msgs("This function was removed from pyCarol")

        return None
