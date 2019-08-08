from string import Formatter


class Carolina:
    """
    This class is used to handle any iteration with Carol data storage (CDS).

    Args:
        carol: class: pycarol.Carol
    """

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
        """
        Initialize GCP back-end

        Args:
            token: `dict`
            TODO: add here the fields for GCP
                GCP token
                {
                    "aiAccessKeyId": aws_access_key_id,
                    "aiSecretKey": aws_secret_access_key,
                    "aiAccessToken": aws_session_token,
                }

        Returns:
            None

        """
        from google.oauth2 import service_account
        from google.cloud import storage

        self.token = token['token']
        gcp_credentials = service_account.Credentials.from_service_account_info(token['token'])
        self.client = storage.Client(credentials=gcp_credentials, project=token['token']['project_id'])

    def _init_aws(self, token):
        """
                Initialize AWS back-end

                Args:
                    token: `dict`
                        AWS token:
                        {
                            "aiAccessKeyId": aws_access_key_id,
                            "aiSecretKey": aws_secret_access_key,
                            "aiAccessToken": aws_session_token,
                        }

                Returns:
                    None

                """
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
        """
        Format the bucket path for each possible space.

        Args:
            space:  `str`,
                Which bucket to get. Possible values:
                "golden": Data Model golden records.
                "staging": Staging records path
                "staging_master": Staging records from Master
                "staging_rejected": Staging records from Rejected
                "view": Data Model View records
                "app": App  bucket

        Returns:
            formatted bucket path

        """

        # TODO: we can use a dictionary or instead of ifs.
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
        """
                Format the bucket path for each possible space.

                Args:
                    space:  `str`,
                        Which bucket to get. Possible values:
                        "golden": Data Model golden records.
                        "staging": Staging records path
                        "staging_master": Staging records from Master
                        "staging_rejected": Staging records from Rejected
                        "view": Data Model Relationship View records
                        "app": App  bucket
                    vars: `dict`
                        Parameters needed to format the storage path. Possible keys:
                        "tenant_id": Tenant ID.
                        "connector_id": Connector ID
                        "staging_type": Staging Name
                        "dm_name": Data model Name
                        "relationship_view_name": Relationship view name
                        "app_name": App  name


                Returns:
                    formatted path

                """
        vars['tenant_id'] = self.carol.tenant['mdmId']

        # TODO: we can use a dictionary or instead of ifs.
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
