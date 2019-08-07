from string import Formatter


class Carolina:
    token = None

    def __init__(self, carol):
        self.carol = carol
        self.client = None
        self.engine = None
        self.token = None
        self.cds_app_storage_path = None
        self.cds_golden_path = None
        self.cds_staging_path = None
        self.cds_staging_master_path = None
        self.cds_staging_rejected_path = None

    def init_if_needed(self):
        if self.client:
            return

        if Carolina.token is None:
            token = self.carol.call_api('v1/storage/storage/token', params={'carolAppName': self.carol.app_name})
            token['tenant_name'] = self.carol.tenant['mdmName']
            Carolina.token = token
        elif Carolina.token.get('tenant_name', '') != self.carol.tenant['mdmName']:
            token = self.carol.call_api('v1/storage/storage/token', params={'carolAppName': self.carol.app_name})
            token['tenant_name'] = self.carol.tenant['mdmName']
            Carolina.token = token

        token = Carolina.token
        self.engine = token['engine']

        self.cds_app_storage_path = token['cdsAppStoragePath']
        self.cds_golden_path = token['cdsGoldenPath']
        self.cds_staging_path = token['cdsStagingPath']
        self.cds_staging_master_path = token['cdsStagingMasterPath']
        self.cds_staging_rejected_path = token['cdsStagingRejectedPath']
        self.cds_view_path = token['cdsViewPath']

        if self.engine == 'GCP-CS':
            self._init_gcp(token)
        elif self.engine == 'AWS-S3':
            self._init_aws(token)

    def _init_gcp(self, token):
        from google.oauth2 import service_account
        from google.cloud import storage

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

        boto3.setup_default_session(aws_access_key_id=ai_access_key_id, aws_secret_access_key=ai_secret_key,
                                    aws_session_token=ai_access_token)
        self.client = boto3.resource('s3')

    def get_client(self):
        self.init_if_needed()
        return self.client

    def get_bucket_name(self, space):
        if space == 'golden':
            template = self.cds_golden_path['bucket']
        elif space == 'staging':
            template = self.cds_staging_path['bucket']
        elif space == 'staging_master':
            template = self.cds_staging_master_path['bucket']
        elif space == 'staging_rejected':
            template = self.cds_staging_rejected_path['bucket']
        elif space == 'view':
            template = self.cds_view_path['bucket']
        elif space == 'app':
            template = self.cds_app_storage_path['bucket']

        name = Formatter().vformat(template, None, {'tenant_id': self.carol.tenant['mdmId']})
        return name

    def get_path(self, space, vars):
        vars['tenant_id'] = self.carol.tenant['mdmId']

        if space == 'golden':
            template = self.cds_golden_path['path'] + '/'
        elif space == 'staging':
            template = self.cds_staging_path['path'] + '/'
        elif space == 'staging_master':
            template = self.cds_staging_master_path['path'] + '/'
        elif space == 'staging_rejected':
            template = self.cds_staging_rejected_path['path'] + '/'
        elif space == 'view':
            template = self.cds_view_path['path'] + '/'
        elif space == 'app':
            template = self.cds_app_storage_path['path'] + '/'


        name = Formatter().vformat(template, None, vars)
        return name
