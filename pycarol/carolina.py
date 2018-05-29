import boto3


class Carolina:
    def __init__(self, carol):
        self.carol = carol
        self.ai_access_key_id = None
        self.ai_secret_key = None
        self.ai_access_token = None
        self.ai_token_expiration_date = None
        self.s3 = None

    def _init_if_needed(self):
        response = self.carol.call_api('v1/carolina/carolina/token', params={'carolAppName': self.carol.app_name})
        self.ai_access_key_id = response['aiAccessKeyId']
        self.ai_secret_key = response['aiSecretKey']
        self.ai_access_token = response['aiAccessToken']
        self.ai_token_expiration_date = response['aiTokenExpirationDate']

        boto3.setup_default_session(aws_access_key_id=self.ai_access_key_id, aws_secret_access_key=self.ai_secret_key,
                                    aws_session_token=self.ai_access_token)
        self.s3 = boto3.resource('s3')

    def get_s3(self):
        self._init_if_needed()
        return self.s3
