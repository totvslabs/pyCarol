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