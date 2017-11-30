## Please create a folder called data in the directory. It will be used to save some jsons.


user_from  = '***@totvs.com.br'
pw_from   = '*****'
subdomain_from = '****'
token_from = get_token(user_from,pw_from,subdomain_from)
accessToken_from = json.loads(token_from.text)['access_token']

user_to  = '****@totvs.com.br'
pw_to    = '***'
subdomain_to = '***'
token_to = get_token(user_to,pw_to,subdomain_to)
accessToken_to = json.loads(token_to.text)['access_token']


#Creating DataModels

DT_list = get_DataModels(accessToken_from,subdomain_from)
createDMfromSnapshot(accessToken_to,subdomain_to,DT_list)


#Creating Connectors
result, all_mappings, new_connector,schema_list,response_stag = cloneConnectors(accessToken_from,subdomain_from, accessToken_to,subdomain_to,user_to,pw_to)

#Creating NamedQueries
namedQueries = get_GetNamedQueries(accessToken_from,subdomain_from)
creatingNamedQueries(accessToken_to,subdomain_to,namedQueries)





