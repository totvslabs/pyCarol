from .ApiKeyAuth_cloner import ApiKeyAuthCloner


class ApiKeyAuth:
    def __init__(self, api_key):
        self.api_key = api_key
        self.carol = None
        self.connector_id = None
        
    def set_connector_id(self, connector_id):
        self.connector_id = connector_id
    
    def cloner(self):
        return ApiKeyAuthCloner(self)

    def login(self, carol):
        self.carol = carol
        pass

    def authenticate_request(self, headers):
        headers['x-auth-key'] = self.api_key
        headers['x-auth-connectorid'] = self.connector_id

    def switch_context(self, env_id):
        """
        Switch context to an environment within the same organization.

        Args:
            env_id: environment id to switch context to.

        Returns:
            None

        """

        path = f'v2/oauth2/switchTenantContextWithApiKey/{env_id}'
        resp = self.carol.call_api(method='POST', path=path)

        self.api_key = resp['apiKey']
        self.connector_id = resp['connectorId']

    def switch_org_context(self, org_id):
        """
        Go to the organization context or switch organization.

        Args:
            org_id: organization id.

        Returns:
            None

        """

        path = f'v2/oauth2/switchOrgContextWithApiKey/{org_id}'
        resp = self.carol.call_api(method='POST', path=path)

        self.api_key = resp['apiKey']
        self.connector_id = resp['connectorId']


