class ApiKeyAuth:
    def __init__(self, api_key):
        self.api_key = api_key
        self.carol = None
        self.connector_id = None
        
    def setConnectorId(self, connector_id):
        self.connector_id = connector_id
    
    def login(self):
        pass

    def authenticate_request(self, headers):
        headers['x-auth-key'] = self.api_key
        headers['x-auth-connectorid'] = self.connector_id