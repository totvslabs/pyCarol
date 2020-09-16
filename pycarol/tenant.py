from .utils.deprecation_msgs import _deprecation_msgs


class Tenant:
    def __init__(self, carol):
        self.carol = carol

    def get_tenant_by_domain(self, domain, auth=True):
        """
        Get tenant information.

        Args:
            domain: `str`
                Tenant name
            auth: `bool` default `True`
                This API ca be called without being logged in. If auth=False it will not use token to call it.

        Returns:
            dict with the information about the tenant.
        """
        return self.carol.call_api('v2/tenants/domain/{}'.format(domain), auth=auth, status_forcelist=[], retries=0)


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
