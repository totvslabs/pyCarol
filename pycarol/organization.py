class Organization:
    """
    Get organization information.

    Args:

        Carol object
            Carol object.
    """

    def __init__(self, carol):
        self.carol = carol

    def get_organization_info(self, organization, auth=True):
        """

        Args:
            organization: `str`
                Organization subdomain
            auth: `bool`
            If send the auth information. Some fields do not come if not logged.

        Returns:
             dict with the information about the organization.

        """

        return self.carol.call_api(f'v1/organizations/domain/{organization}',
                                   auth=auth, status_forcelist=[], retries=0)

    def get_environment_info(self, environment, auth=True):
        """
        Get environment information.

        Args:
            environment: `str`
                Tenant name
            auth: `bool`
                If send the auth information. Some fields do not come if not logged.

        Returns:
            dict with the information about the environment.

        """

        return self.carol.call_api(f'v2/tenants/domain/{environment}',
                                   auth=auth, status_forcelist=[], retries=0)
