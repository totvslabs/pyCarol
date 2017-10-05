import json
import requests
from . import utils


class sendDataCarol:

    def __init__(self, token_object):

        self.token_object = token_object
        self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
        self.stagingName = None
        self.step_size = 100
        self.url_filter = None
        self.print_stats = False

    def _streamData(self,data):
        i = 0
        size_data = len(data)
        while not i >= size_data:
            action = yield data[i:i + self.step_size]
            if action is None:
                i += self.step_size
        yield []

    def sendData(self, stagingName,data = None, print_stats = False):

        self.print_stats = print_stats
        if not data:
            assert not self.data==[]
        else:
            self.data = data


        self.stagingName = stagingName
        self.url_filter = "https://{}.carol.ai/api/v2/staging/tables/{}?returnData=false&applicationId={}" \
            .format(self.token_object.domain, self.stagingName, self.token_object.applicationId)


        gen = self._streamData(self.data)

        data_size = len(self.data)
        cont = 0
        ite = True
        data_json = gen.__next__()
        while ite:
            response = requests.post(url=self.url_filter, headers=self.headers, json=data_json)
            if not response.ok:
                # error handler for token
                if response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                    data_json = gen.send(True)
                    continue
                raise Exception(response.reason)

            cont += len(data_json)
            if self.print_stats:
                print('{}/{} sent'.format(cont,data_size), end ='\r')
            data_json = gen.__next__()
            if data_json ==[]:
                ite = False

    @classmethod
    def from_json(cls, token, filename):

        with open(filename, 'r') as data_file:
            json_data = data_file.read()

        data = json.loads(json_data)
        ret = sendDataCarol(token)
        ret.data = data
        return ret