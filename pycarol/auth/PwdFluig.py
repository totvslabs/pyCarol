import urllib.parse

from bs4 import BeautifulSoup
import requests

from .PwdKeyAuth import PwdKeyAuth
from ..exceptions import InvalidToken, CarolApiResponseException


class PwdFluig(PwdKeyAuth):
    """

    Args:

        user: `str`
            Username fluig identity
        password: `str`
            Password
    """

    def __init__(self, user, password, lazy_login=False):
        self.user = user
        self.password = password
        self._token = None
        self.carol = None
        self.connector_id = None
        self.sess = requests.Session()
        self.lazy_login = lazy_login

    def _fluig_login(self, username, password):
        url = "https://totvs.fluigidentity.com/api/auth/v1/login"

        login_data = {
            'domain': 'totvs',
            'grant_type': 'password',
            'username': username,
            'password': password,
            'captchaReturnedValue': 'undefined',
        }
        headers = {
            'accept': 'application/json',
            'content-type': 'application/x-www-form-urlencoded',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        }

        response = self.sess.request("POST", url, headers=headers,
                                     data=login_data, allow_redirects=True)

        if not response.ok:
            raise InvalidToken(response.text)
        return response

    def login(self, carol):
        """

        Args:

            carol: pycarol.Carol
                Carol() instance.

        Returns:
            None

        """
        self.carol = carol

        params = params = {
            "redirect": f"/{self.carol.domain}/carol-ui/carol-ui/",
            "org": f"{self.carol.organization}",
            "env": f"{self.carol.domain}",
        }
        headers = {}
        headers.update({'User-Agent': self.carol._user_agent})
        url = f'https://{self.carol.organization}.carol.ai/samlAuth/'

        resp = self.sess.get(url, headers=headers,
                             allow_redirects=True, params=params)

        soup = BeautifulSoup(resp.text, 'html.parser')
        tag = soup.find("form")
        if tag is None:
            raise Exception("Unable to login using fluig")

        tag = urllib.parse.unquote(
            tag['action'].replace('/cloudpass/?forward=%2F', ''))
        next_redirect = "https://totvs.fluigidentity.com/cloudpass/" + tag

        token = self._fluig_login(self.user, self.password)

        for i, j in token.json().items():
            self.sess.cookies[i] = str(j)

        resp = self.sess.get(
            next_redirect, headers=headers, allow_redirects=True)

        soup = BeautifulSoup(resp.text, 'html.parser')
        payload = {}
        for tag in soup.find_all("input", type="hidden", attrs={"name": ["RelayState", "SAMLResponse", "Signature", "SigAlg", "KeyInfo"]}):
            payload[tag['name']] = tag['value']

        url = f"https://{self.carol.organization}.carol.ai/api/v1/saml/ACS?orgSubdomain={self.carol.organization}"
        headers['content-type'] = 'application/x-www-form-urlencoded'
        carol_resp = self.sess.request(
            "POST", url, headers=headers, data=payload, allow_redirects=False)

        if not carol_resp.ok:
            raise CarolApiResponseException(carol_resp.text)
        self.access_token = carol_resp.headers['Location'].split(
            'handoff=')[-1]

        super().login(self.carol)
