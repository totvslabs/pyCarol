

import json
from pycarol import queriesCarol, loginCarol, namedQueryCarol
import requests

with open('E:\\carol-ds-retail\\config.json') as json_data:
    d = json.loads(json_data.read())

token_object = loginCarol.loginCarol(**d['bematechn'])
token_object.newToken()
query = namedQueryCarol.namedQueries(token_object)
query.getParamByName('revenueHist')