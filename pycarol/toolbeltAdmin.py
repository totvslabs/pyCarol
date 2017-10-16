import requests

from pycarol.queriesCarol import queryCarol


class deleteEntryCarol:
    def __init__(self, token_object):
        self.token_object = token_object
        self.indexType = 'MASTER'
        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.query_data = []
        self.querystring = {"indexType": self.indexType, "tenantId": self.token_object.tenantId}

    def deleteRegisters(self, json_query, indexType='MASTER',
                        print_status=True):
        self.indexType = indexType
        self.query_data = []

        check_register = queryCarol(self.token_object).checkTotalHits(json_query)

        self.querystring = {"indexType": self.indexType, "tenantId": self.token_object.tenantId}

        _deleted = True
        self.totalHits = check_register.totalHits
        while _deleted:
            url_filter = "https://{}.carol.ai/api/v1/databaseToolbelt/filter".format(self.token_object.domain)
            self.lastResponse = requests.delete(url=url_filter, headers=self.headers, params=self.querystring,
                                                json=json_query)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            _deleted = False
