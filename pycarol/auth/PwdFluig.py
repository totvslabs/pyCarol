import urllib.parse

from bs4 import BeautifulSoup
import requests
import json
from pathlib import Path

from .PwdKeyAuth import PwdKeyAuth
from ..exceptions import InvalidToken, CarolApiResponseException
from .. import __TEMP_STORAGE__


class PwdFluig(PwdKeyAuth):
    """

    Args:

        user: `str`
            Username fluig identity
        password: `str`
            Password
    """

    def __init__(self, user, lazy_login=False, access_token=None):
        self.user = user
        self.access_token = access_token
        self._token = None
        self.carol = None
        self.connector_id = None
        self.sess = requests.Session()
        self.lazy_login = lazy_login


    def login(self, carol):
        """

        Args:

            carol: pycarol.Carol
                Carol() instance.

        Returns:
            None

        """
        self.carol = carol
        self._temp_file_name = f".pycarol_token_{self.user}_{self.carol.organization}_{self.carol.domain}.json"
        self._tmp_filepath = Path(__TEMP_STORAGE__) / self._temp_file_name

        if self._tmp_filepath.exists():
            with open(self._tmp_filepath, "r", encoding="utf-8") as file:
                token_data = json.load(file)
                self._set_token(token_data)

                # Try access token
                resp = self.carol.call_api(f'v5/oauth2/token/{token_data["access_token"]}', auth=False, errors="ignore")
                if resp.get('access_token') and resp.get('errorCode') is None:
                    return
                
                # Try refresh token
                try:
                    self._refresh_token()
                    return
                except Exception:
                    pass
        
        access_token = input(f'Please login into https://{self.carol.organization}.carol.ai/{self.carol.domain} and provide an access_token: ') if self.access_token is None else self.access_token

        headers = {'Authorization': access_token}

        token_details = carol.call_api(f'v5/oauth2/token/{access_token}/userAccessDetails', auth=False,
                             extra_headers=headers)
        
        provided_tenant = token_details['tenantId']
        provided_org = token_details['orgId']
        provided_user = token_details['userId']

        provided_tenant_details = carol.call_api(f'v5/tenants/{provided_tenant}', auth=False,
                                extra_headers=headers)        
        provided_org_details = carol.call_api(f'v3/organizations/{provided_org}', auth=False,
                                extra_headers=headers)
        provided_user_details = carol.call_api(f'v3/users/{provided_user}', auth=False,
                                extra_headers=headers)
        
        check = (
            self.carol.domain == provided_tenant_details['mdmSubdomain'] and
            self.carol.organization == provided_org_details['mdmSubdomain'] and
            self.user == provided_user_details['mdmUserLogin']
        )

        if not check:
            raise InvalidToken('Please provide correct credentials.')
        
        token = carol.call_api(f'v5/oauth2/token/{access_token}', auth=False,
                            extra_headers=headers)
        
        self._set_token(token)

        if self.carol.verbose:
            print("Token: {}".format(self._token.access_token))