
import json
from pycarol import loginCarol, connectorsCarol, queriesCarol


with open('E:\\carol-ds-retail\\config.json') as json_data:
    d = json.loads(json_data.read())

token_object = loginCarol.loginCarol(**d['mario'])
token_object.newToken()
print(token_object.access_token)

#conn = applicationsCarol.connectorsCarol(token_object)

#conn.createConnector(connectorName = 'tes123a', connectorLabel = None, groupName = "Oth234")
#connectorId = conn.connectorId
#conn.getConnectorsByName('rest_api_connector')

query_response = queriesCarol.queryCarol(token_object)
query_response.namedQueryParams('studentsByState3')