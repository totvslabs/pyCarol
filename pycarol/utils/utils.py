import requests
import json

def scrub(obj, bad):
    '''
    remove recursively a list of keys in a dictionary.  
    :param obj: dictionary
    :param bad: list of keys
    :return: empty
    '''
    if isinstance(obj, dict):
        iter_k = list(obj.keys())
        for kk in reversed(range(len(iter_k))):
            k = iter_k[kk]
            if k in bad:
                del obj[k]
            else:
                scrub(obj[k], bad)
    elif isinstance(obj, list):
        for i in reversed(range(len(obj))):
            if obj[i] in bad:
                del obj[i]
            else:
                scrub(obj[i], bad)

    else:
        # neither a dict nor a list, do nothing
        pass



class tenantsCarol:
    def __init__(self,token_object):
        self.dev = token_object.dev
        self.token_object = token_object
        self.headers = self.token_object.headers_to_use

    def getInfo(self,domain):
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