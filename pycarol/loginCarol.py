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

    def __init__(self, username= None , password= None, domain = None , connectorId='0a0829172fc2433c9aa26460c31b78f0',
                 X_Auth_Key= None, X_Auth_ConnectorId= None ):


        self.username = username
        self.password = password
        self.domain = domain
        self.connectorId = connectorId
        self.access_token = None
        self.refresh_token = None
        self.dev = ''
        self.X_Auth_Key = X_Auth_Key
        self.X_Auth_ConnectorId = X_Auth_ConnectorId
        self.headers_to_use =None
        self.use_APIkey =  None


        assert self.domain is not None

        if self.username is None or self.password is None:
            assert self.X_Auth_ConnectorId and self.X_Auth_Key is not None
            self.use_APIkey = True
            self.headers_to_use = {'x-auth-key': self.X_Auth_Key, 'x-auth-connectorid': self.X_Auth_ConnectorId,
                                   'Content-Type': 'application/json'}
        else:
            self.use_APIkey = False
            self.headers_to_use = {'Authorization': self.access_token, 'Content-Type': 'application/json'}


    def getAPIKey(self, connectorId=None):

        if connectorId is not None:
            self.connectorId = connectorId

        if self.access_token is None:
            self.newToken()

        self.headers = {'Authorization': self.access_token, 'Content-Type': 'application/x-www-form-urlencoded'}

        url = "https://{}.carol.ai{}/api/v2/apiKey/issue".format(self.domain, self.dev)
        payload = "connectorId={}".format(self.connectorId)

        token = requests.request("POST", url, data=payload, headers= self.headers)
        if token.ok:
            self.X_Auth_Key = json.loads(token.text)['X-Auth-Key']
            self.X_Auth_ConnectorId = json.loads(token.text)['X-Auth-ConnectorId']
            self.headers_to_use = {'x-auth-key': self.X_Auth_Key, 'x-auth-connectorid': self.X_Auth_ConnectorId,
                                   'Content-Type': 'application/json'}
            self.connectorId = self.X_Auth_ConnectorId
            return token
        else:
            raise Exception(token.text)



    def getAPIKeyDetails(self, X_Auth_Key=None, X_Auth_ConnectorId=None):

        if X_Auth_Key is not None:
            self.X_Auth_Key = X_Auth_Key

        if X_Auth_ConnectorId is not None:
            self.X_Auth_ConnectorId = X_Auth_ConnectorId

        if (self.X_Auth_Key) is None or (self.X_Auth_ConnectorId) is None:
            if self.access_token is None:
                self.newToken()
            self.headers = {'Authorization': self.access_token, 'Content-Type': 'application/x-www-form-urlencoded'}
        else:
            self.headers = {'x-auth-key': self.X_Auth_Key, 'x-auth-connectorid': self.X_Auth_ConnectorId,
                            'Content-Type': 'application/json'}


        url = "https://{}.carol.ai{}/api/v2/apiKey/details".format(self.domain, self.dev)
        querystring = {"apiKey": self.X_Auth_Key, "connectorId": self.X_Auth_ConnectorId}
        token = requests.request("GET", url, params=querystring, headers= self.headers)
        if token.ok:
            self.APIKeyDetails = json.loads(token.text)
            return self.APIKeyDetails
        else:
            raise Exception(token.text)

    def revokeAPIKey(self, X_Auth_Key = None, X_Auth_ConnectorId = None):

        if X_Auth_Key is not None:
            self.X_Auth_Key = X_Auth_Key

        if X_Auth_ConnectorId is not None:
            self.X_Auth_ConnectorId = X_Auth_ConnectorId

        assert self.X_Auth_ConnectorId is not None
        assert self.X_Auth_Key is not None
        assert self.access_token is not None

        self.headers = {'Authorization': self.access_token, 'Content-Type': 'application/x-www-form-urlencoded'}

        payload = "apiKey={}&connectorId={}".format(self.X_Auth_Key, self.X_Auth_ConnectorId)
        url = "https://{}.carol.ai{}/api/v2/apiKey/revoke".format(self.domain, self.dev)

        token = requests.request("POST", url, data=payload, headers=self.headers)
        if token.ok:
            print('APYKey: {} was revoked'.format(self.X_Auth_Key))
        else:
            raise Exception(token.text)

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
            self.connectorId = connectorId
        url = 'https://{}.carol.ai{}/api/v2/oauth2/token'.format(self.domain, self.dev)

        grant_type = 'password'  # use refresh_token if one wants to refresh the token
        refresh_token = ''  # pass if refresh the token is needed

        auth_request = {'username': self.username, 'password': self.password, "grant_type": grant_type, 'subdomain': self.domain,
                        'connectorId': self.connectorId, 'refresh_token': refresh_token, 'Content-Type': 'application/json'}
        token = requests.post(url=url, data=auth_request)
        if token.ok:
            self.access_token = json.loads(token.text)['access_token']
            self.refresh_token = json.loads(token.text)['refresh_token']
            self.tenantId = json.loads(token.text)['client_id'].split('_')[0]
            self.headers_to_use = {'Authorization': self.access_token, 'Content-Type': 'application/json'}
            return self
        else:
            raise Exception(token.text)

    def refreshToken(self):
        """
        Refresh a token.
        Usage::
          >>> from pycarol.loginCarol import  loginCarol
          >>> login = loginCarol(username= username, password=my_password, domain = my_domain, connectorId=my_connectorId)
          >>> login.refreshToken()
        """
        url = 'https://{}.carol.ai{}/api/v2/oauth2/token'.format(self.domain, self.dev)
        grant_type = 'refresh_token'  # use refresh_token if one wants to refresh the token
        auth_request = {"grant_type": grant_type, 'refresh_token': self.refresh_token, 'Content-Type': 'application/json'}
        token = requests.post(url=url, data=auth_request)
        if token.ok:
            self.access_token = json.loads(token.text)['access_token']
            self.refresh_token = json.loads(token.text)['refresh_token']
            self.headers_to_use = {'Authorization': self.access_token, 'Content-Type': 'application/json'}
        elif ('Cannot find the provided refresh' in token.text):
            self.newToken()
        else:
            raise Exception(token.text)

    def checkToken(self):
        """
        Token expires information
        Usage::
          >>> from pycarol.loginCarol import  loginCarol
          >>> login = loginCarol(username= username, password=my_password, domain = my_domain, connectorId=my_connectorId)
          >>> login.checkToken()
          The access token '73685500c93011e789910e4789ade3a3' will expires in 3316s
        """
        url = 'https://{}.carol.ai{}/api/v2/oauth2/token/{}'.format(self.domain, self.dev, self.access_token)
        token = requests.get(url=url)
        if token.ok:
            self._expires_in = json.loads(token.text)['expires_in']
            print("The access token '{}' will expires in {}s".format(self.access_token,self._expires_in))
        else:
            raise Exception(token.reason)
