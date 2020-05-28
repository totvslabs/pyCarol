class Organization:
    """
    Get organization information.

    Args:

        Carol object
            Carol object.
    """

    def __init__(self, carol):

        self.carol = carol

    def get_organization_info(self, organization):
        """
        Get organization information.

        :param organization: `str`
            Organization subdomain
        :return:
            dict with the information about the organization.
        """

        return self.carol.call_api(f'v1/organizations/domain/{organization}',
                                   auth=False, status_forcelist=[], retries=0)


    def get_environment_info(self, environment):
        """
        Get environment information.

        :param environment: `str`
            Tenant name
        :return:
            dict with the information about the environment.
        """
        return self.carol.call_api(f'v2/tenants/domain/{environment}',
                                   auth=False, status_forcelist=[], retries=0)
