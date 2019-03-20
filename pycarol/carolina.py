class Carolina:
    def __init__(self, carol):
        self.carol = carol
        self.client = None
        self.engine = None
        self.bucketName = None
        self.token = None

    def init_if_needed(self):
        if self.client:
            return

        token = self.carol.call_api('v1/storage/storage/token')

        self.engine = token['engine']

        if self.engine == 'GCP-CS':
            self._init_gcp(token)
        elif self.engine == 'AWS-S3':
            self._init_aws(token)

    def _init_gcp(self, token):
        from google.oauth2 import service_account
        from google.cloud import storage

        self.bucketName = token['bucketName']
        self.token = token['token']
        gcp_credentials = service_account.Credentials.from_service_account_info(token['token'])
        self.client = storage.Client(credentials=gcp_credentials, project=token['token']['project_id'])

    def _init_aws(self, token):
        import boto3
        self.token = token
        ai_access_key_id = token['aiAccessKeyId']
        ai_secret_key = token['aiSecretKey']
        ai_access_token = token['aiAccessToken']
        ai_token_expiration_date = token['aiTokenExpirationDate']
        self.bucketName = token['s3Bucket']

        boto3.setup_default_session(aws_access_key_id=ai_access_key_id, aws_secret_access_key=ai_secret_key,
                                    aws_session_token=ai_access_token)
        self.client = boto3.resource('s3')

    def get_client(self):
        self.init_if_needed()
        return self.client
