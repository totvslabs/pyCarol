import json
import requests



class tenantInfo:
    def __init__(self,token_object):
        self.dev = token_object.dev
        self.token_object = token_object
        self.headers = self.token_object.headers_to_use

    def getInfo(self,domain= None):

        if domain is None:
            domain = self.token_object.domain
        _true = True
        url = "https://{}.carol.ai{}/api/v2/tenants/domain/{}".format(self.token_object.domain,self.dev,domain)
        while _true:
            self.response = requests.request("GET", url, headers=self.headers)
            if not self.response.ok:
                # error handler for token
                if self.response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.response.text)
            self.response.encoding = 'utf8'
            self.tenantInfo = json.loads(self.response.text)
            _true =False
        return self.tenantInfo
