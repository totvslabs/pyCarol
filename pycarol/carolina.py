from string import Formatter
from datetime import datetime, timedelta

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
        self.expires_at = None
        self.cds_app_storage_path = None
        self.cds_golden_path = None
        self.cds_staging_path = None
        self.cds_staging_master_path = None
        self.cds_staging_rejected_path = None

    def init_if_needed(self):
        expired = False
        if self.client:
            # Check if the token is not expired for at least another minute.. we do a little margin to avoid time difference issues
            if self.expires_at is None or datetime.utcnow() + timedelta(minutes=1) < self.expires_at:
                return
            else:
                expired = True

        if Carolina.token is None or expired or Carolina.token.get('tenant_name', '') != self.carol.tenant['mdmName'] \
                or Carolina.token.get('app_name', '') != self.carol.app_name \
                or (datetime.utcnow() + timedelta(minutes=1)
                    >= datetime.fromtimestamp(Carolina.token.get('expirationTimestamp', 1)/1000.0)):
            token = self.carol.call_api('v1/storage/storage/token', params={'carolAppName': self.carol.app_name})
            token['tenant_name'] = self.carol.tenant['mdmName']
            token['app_name'] = self.carol.app_name
            Carolina.token = token

        token = Carolina.token
        self.engine = token['engine']

        if token.get('expirationTimestamp', None) is not None:
            self.expires_at = datetime.fromtimestamp(token['expirationTimestamp'] / 1000.0)

        self.cds_app_storage_path = token['cdsAppStoragePath']
        self.cds_golden_path = token['cdsGoldenPath']
        self.cds_staging_path = token['cdsStagingPath']
        self.cds_staging_master_path = token['cdsStagingMasterPath']
        self.cds_staging_rejected_path = token['cdsStagingRejectedPath']
        self.cds_view_path = token['cdsViewPath']

        self.cds_golden_intake_path = token['cdsIntakeGoldenPath']
        self.cds_staging_intake_path = token['cdsIntakeStagingPath']

        self.cds_golden_rejected_intake_path = token['cdsRejectedPath']

        self.cds_view_intake_path = token['cdsIntakeViewPath']

        if self.engine == 'GCP-CS':
            self._init_gcp(token)

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

    def get_client(self):
        self.init_if_needed()
        return self.client

    def get_bucket_name(self, space):
        """
        Format the bucket path for each possible space.

        Args:

            space:  `str`,
                Which bucket to get. Possible values:

                    1. "golden": Data Model golden records.
                    2. "staging": Staging records path
                    3. "staging_master": Staging records from Master
                    4. "staging_rejected": Staging records from Rejected
                    5. "view": Data Model View records
                    6. "app": App  bucket
                    7. "golden_cds": CDS golden records
                    8. "staging_cds": Staging Intake.

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
        elif space == 'staging_cds':
            template = self.cds_staging_intake_path['bucket']
        elif space == 'golden_cds':
            template = self.cds_golden_intake_path['bucket']
        elif space == 'view_cds':
            template = self.cds_view_intake_path['bucket']
        elif space == 'golden_rejected':
            template = self.cds_staging_rejected_path['bucket']
        else:
            raise ValueError

        name = Formatter().vformat(template, None, {'tenant_id': self.carol.tenant['mdmId']})
        return name

    def get_path(self, space, vars):
        """
                Format the bucket path for each possible space.

                Args:

                    space:  `str`,
                        Which bucket to get. Possible values:

                        1. "golden": Data Model golden records.
                        2. "staging": Staging records path
                        3. "staging_master": Staging records from Master
                        4. "staging_rejected": Staging records from Rejected
                        5. "view": Data Model Relationship View records
                        6. "app": App  bucket
                        7. "golden_cds": CDS golden records
                        8. "staging_cds": Staging Intake.
                    vars: `dict`
                        Parameters needed to format the storage path. Possible keys:

                        1. "tenant_id": Tenant ID.
                        2. "connector_id": Connector ID
                        3. "staging_type": Staging Name
                        4. "dm_name": Data model Name
                        5. "relationship_view_name": Relationship view name
                        6. "app_name": App  name


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
        elif space == 'staging_cds':
            template = self.cds_staging_intake_path['path'] + '/'
        elif space == 'golden_cds':
            template = self.cds_golden_intake_path['path'] + '/'
        elif space == 'view_cds':
            template = self.cds_view_intake_path['path'] + '/'
        elif space == 'golden_rejected':
            template = self.cds_golden_rejected_intake_path['path'] + '/'
        else:
            raise ValueError

        name = Formatter().vformat(template, None, vars)
        return name

