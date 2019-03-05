class Carolina:
    def __init__(self, carol):
        self.carol = carol
        self.gcp_token = None
        self.storage = None

    def _init_if_needed(self):
        from google.oauth2 import service_account
        from google.cloud import storage

        gcp_token = self.carol.call_api('v1/carolina/carolina/token')
        self.gcp_credentials = service_account.Credentials.from_service_account_info(gcp_token)

        self.storage = storage.Client(credentials=self.gcp_credentials, project=gcp_token['project_id'])

    def get_storage(self):
        self._init_if_needed()
        return self.storage
