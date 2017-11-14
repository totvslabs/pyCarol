import json
import requests


class loginCarol:
    """ It implements the calls for the following end ppoints:
       1. /api/v2/oauth2/token
       2. /api/v2/oauth2/token/{access_token}

    :param username: The tenant user name
    :param password: Tenant password
    :param domain: Tenant domain, e.g., for the link  mytenant.carol.ai, the domain is "mytenant"
    :param connectorId: connector id to login, the default value allows only queries and low level calls.
    :return:
    Carol login object.
    Usage::
      >>> from pycarol.loginCarol import  loginCarol
      >>> login = loginCarol(username= username, password=my_password, domain = my_domain, connectorId=my_connectorId)
    """

    def __init__(self, username, password, domain, connectorId='0a0829172fc2433c9aa26460c31b78f0'):
        self.username = username
        self.password = password
        self.domain = domain
        self.applicationId = connectorId
        self.access_token = None
        self.refresh_token = None

    def newToken(self, connectorId=None):
        """
        Generate an access token.
        :param connectorId: A new connectorId to be used to generate the access token
        Usage::
          >>> from pycarol.loginCarol import  loginCarol
          >>> login = loginCarol(username= username, password=my_password, domain = my_domain, connectorId=my_connectorId)
          >>> login.newToken()
          <Response [200]>
        """

        if connectorId is not None:
            self.applicationId = connectorId
        url = 'https://{}.carol.ai/api/v2/oauth2/token'.format(self.domain)

        grant_type = 'password'  # use refresh_token if one wants to refresh the token
        refresh_token = ''  # pass if refresh the token is needed

        auth_request = {'username': self.username, 'password': self.password, "grant_type": grant_type, 'subdomain': self.domain,
                        'connectorId': self.applicationId, 'refresh_token': refresh_token, 'Content-Type': 'application/json'}
        token = requests.post(url=url, data=auth_request)
        if token.ok:
            self.access_token = json.loads(token.text)['access_token']
            self.refresh_token = json.loads(token.text)['refresh_token']
            self.tenantId = json.loads(token.text)['client_id'].split('_')[0]
            return token
        else:
            raise Exception(token.reason)

    def refreshToken(self):
        """
        Refresh a token.
        Usage::
          >>> from pycarol.loginCarol import  loginCarol
          >>> login = loginCarol(username= username, password=my_password, domain = my_domain, connectorId=my_connectorId)
          >>> login.refreshToken()
        """
        url = 'https://{}.carol.ai/api/v2/oauth2/token'.format(self.domain)
        grant_type = 'refresh_token'  # use refresh_token if one wants to refresh the token
        auth_request = {"grant_type": grant_type, 'refresh_token': self.refresh_token, 'Content-Type': 'application/json'}
        token = requests.post(url=url, data=auth_request)
        if token.ok:
            self.access_token = json.loads(token.text)['access_token']
            self.refresh_token = json.loads(token.text)['refresh_token']
        else:
            raise Exception(token.reason)

    def checkToken(self):
        url = 'https://{}.carol.ai/api/v2/oauth2/token/{}'.format(self.domain, self.access_token)
        token = requests.get(url=url)
        if token.ok:
            #self.token2 = json.loads(token.text)
            self._expires_in = json.loads(token.text)['expires_in']
            print('The access token {} will expires in {}s'.format(self.access_token,self._expires_in))
        else:
            raise Exception(token.reason)


'''
    def logout(self):
        url = 'https://{}.carol.ai/api/v2/oauth2/logout'.format(self.domain)
        auth_request = {"access_token": self.access_token,'Content-Type': 'application/json'}
        token = requests.post(url=url, data=auth_request)
        if token.ok:
            #self.token2 = json.loads(token.text)
            pass
        else:
            raise Exception(token.reason)
            '''
