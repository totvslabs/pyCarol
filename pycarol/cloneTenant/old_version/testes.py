from functions import *


## Please create a folder called data in the directory. It will be used to save some jsons.


user_from  = 'rafael.rui@totvs.com.br'
pw_from   = 'wk$0M#^yZAO9'
subdomain_from = 'omnistory'
token_from = get_token(user_from,pw_from,subdomain_from)
accessToken_from = json.loads(token_from.text)['access_token']

user_to  = '****@totvs.com.br'
pw_to    = '***'
subdomain_to = '***'
token_to =1
accessToken_to = 1


#Creating DataModels

#DT_list = get_DataModels(accessToken_from,subdomain_from)
#createDMfromSnapshot(accessToken_to,subdomain_to,DT_list)


#Creating Connectors
result, all_mappings, new_connector,schema_list,response_stag = cloneConnectors(accessToken_from,subdomain_from, accessToken_to,subdomain_to,user_to,pw_to)

#Creating NamedQueries
namedQueries = get_GetNamedQueries(accessToken_from,subdomain_from)
creatingNamedQueries(accessToken_to,subdomain_to,namedQueries)


