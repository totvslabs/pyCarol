import json
import requests
#from . import utils
import re


class staginCarol:
    def __init__(self, token_object):
        self.token_object = token_object
        self.offset = 0
        self.pageSize = 50
        self.sortOrder = 'ASC'
        self.sortBy = None
        self.drop_list = None
        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.query_data = []
        self.table = None
        self.querystring = {}
        self.applicationId = self.token_object.applicationId
        self._setQuerystring()

    def _setQuerystring(self):
        if self.sortBy is None:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "applicationId": self.applicationId}
        else:
            self.querystring = {"offset": self.offset, "pageSize": str(self.pageSize), "sortOrder": self.sortOrder,
                                "sortBy": self.sortBy, "applicationId": self.applicationId}

    def getStaging(self, table, applicationId=None, offset=0, pageSize=50, sortOrder='ASC', sortBy='mdmLastUpdated',
                   print_status=True, save_results=True, filename='staging_result.json', safe_check=False):
        self.offset = offset
        self.pageSize = pageSize
        self.sortOrder = sortOrder
        self.table = table
        self.sortBy = sortBy
        self.query_data = []
        if applicationId is not None:
            self.applicationId = applicationId

        self._setQuerystring()

        set_param = True
        count = self.offset
        self.totalHits = float("inf")
        if save_results:
            file = open(filename, 'w', encoding='utf8')
        while count < self.totalHits:
            url_filter = "https://{}.carol.ai/api/v2/staging/tables/{}".format(self.token_object.domain, self.table)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                if save_results:
                    file.close()
                raise Exception(self.lastResponse.text)

            self.lastResponse.encoding = 'utf8'
            query = json.loads(self.lastResponse.text)
            count += query['count']
            if set_param:
                self.totalHits = query["totalHits"]
                set_param = False
                if safe_check:
                    mdmId_list = []
            query = query['hits']
            if safe_check:
                mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                if len(mdmId_list) > len(set(mdmId_list)):
                    raise Exception('There are repeated records')

            self.query_data.extend(query)
            self.querystring['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.totalHits), end ='\r')
            if save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
        if save_results:
            file.close()

    def checkTotalHits(self, table, applicationId=None):
        self.table = table
        self.pageSize = 0
        if applicationId is not None:
            self.applicationId = applicationId

        self._setQuerystring()
        errors = True
        while errors:
            url_filter = "https://{}.carol.ai/api/v2/staging/tables/{}".format(self.token_object.domain, self.table)
            self.lastResponse = requests.get(url=url_filter, headers=self.headers, params=self.querystring)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            errors = False

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        self.totalHits = query["totalHits"]
        return self.totalHits
