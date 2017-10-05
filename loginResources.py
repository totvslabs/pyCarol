import json
import requests


class loginCarol:

    def __init__(self, username, password, domain, applicationId='0a0829172fc2433c9aa26460c31b78f0'):
        self.username = username
        self.password = password
        self.domain = domain
        self.applicationId = applicationId
        self.access_token = None
        self.refresh_token = None

    def newToken(self):
        url = 'https://{}.carol.ai/api/v2/oauth2/token'.format(self.domain)

        grant_type = 'password'  # use refresh_token if one wants to refresh the token
        refresh_token = ''  # pass if refresh the token is needed

        auth_request = {'username': self.username, 'password': self.password, "grant_type": grant_type, 'subdomain': self.domain,
                        'applicationId': self.applicationId, 'refresh_token': refresh_token, 'Content-Type': 'application/json'}
        token = requests.post(url=url, data=auth_request)
        if token.ok:
            self.access_token = json.loads(token.text)['access_token']
            self.refresh_token = json.loads(token.text)['refresh_token']
            self.tenantId = json.loads(token.text)['client_id'].split('_')[0]
            return token
        else:
            raise Exception(token.reason)

    def refreshToken(self):
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
