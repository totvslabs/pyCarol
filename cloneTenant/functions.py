import json
import requests
import numpy as np
# coding:utf-8
import sys  # Reload does the trick!

## Autentication
def get_token(user,pw,subdomain,appId = '0a0829172fc2433c9aa26460c31b78f0'):
    '''
    Get authentication token.
    :param user: username
    :param pw: password
    :param subdomain: tenant name
    :param appId: ApplicationId to be used in the authentication. Default value does not allow to send data to Carol
    :return: Request authentication response. 
    '''
    url = 'https://{}.carol.ai/api/v1/oauth2/token'.format(subdomain)
    grant_type = 'password'  # use refresh_token if one wants to refresh the token
    refresh_token = ''  # pass if refresh the token is needed
    auth_request = {'username': user, 'password': pw,
                    "grant_type": grant_type, 'subdomain': subdomain,
                    'applicationId': appId, 'refresh_token': refresh_token,
                    'Content-Type': 'application/json'
                    }
    token = requests.post(url=url, data=auth_request)
    if token.ok:
        return token
    else:
        raise ValueError('Error: {}'.format(token.text))



#Copy and create Data Models
def get_DataModels(accessToken,tenant,filename='data/DataModels.json'):
    '''
    Copy data model information in the Source tenant. Also, create snapshot for each datamodel
    :param accessToken: AccessToken Source tenant
    :param tenant: Source tenant domain
    :param filename: filename to save the source datamodels
    :return: List of snapshots of each data model (json format). Used to create new data models at the new tenant. Also, create two 
    jsons named as filename.json and filename_snapshot.json
    '''
    pageSize = '-1'
    url_filter = 'https://{}.carol.ai/api/v1/entities/templates/published?offset=0&pageSize={}&sortOrder=ASC'.format(tenant,pageSize)
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    response = requests.get(url=url_filter,headers=headers)
    if response.ok:
        response.encoding='utf-8'
        result = json.loads(response.text,encoding='utf-8')
        if (len(result['hits']) == result['totalHits']):
            print('{} DataModels received'.format(len(result['hits'])))
            result = result['hits']
            with open(filename, 'w',encoding='utf-8') as outfile:
                json.dump(result, outfile, indent=2,ensure_ascii=False)
        else:
            print('{} DataModels received, expected = {}'.format(len(result['hits']),result['totalHits']))
    else:
        print('Error', response.text)
    snapshot_list = []
    f = open(filename.split('.')[0]+'_snapshot.json', 'w',encoding='utf-8')
    f.write('[\n')
    print('Getting snapshots')
    for dm in result:
        entityTemplateId = dm['mdmId']
        entitySpace = dm['mdmEntitySpace']
        url_snapshot = 'https://{}.carol.ai/api/v1/entities/templates/{}/snapshot?entitySpace={}'.format(tenant,entityTemplateId,entitySpace)
        snapshot = requests.get(url=url_snapshot,headers=headers)
        if snapshot.ok:
            snapshot.encoding = 'utf-8'
            snap = json.loads(snapshot.text,encoding='utf-8')
            snapshot_list.append(snap)
            json.dump(snap, f,ensure_ascii=False)
            f.write(",\n")
            line = f.tell()
        else:
            raise ValueError('Error snapshot {}'.format(snapshot.text))
    f.seek(line-3)
    f.write("\n]")
    f.close()
    return snapshot_list

def sendDataModel(token,tenant,snap):
    '''
    Send data model to the new tenant.
    :param token: AccessToken new tenant
    :param tenant: New tenant domain
    :param snap: Snapshot (json format) a data model to be published. 
    :return: request response
    '''
    url = 'https://{}.carol.ai/api/v1/entities/templates/snapshot'.format(tenant)
    headers = {'Authorization': token, 'Content-Type': 'application/json'}
    response = requests.post(url=url,headers=headers, json= snap)
    return response


def publishDataModel(token,entityTemplateId,tenant):
    '''
    Publish data model
    :param token: AccessToken new tenant
    :param entityTemplateId: Entity templateId of the data model created. 
    :param tenant: New tenant domain
    :return: request response
    '''
    url = 'https://{}.carol.ai/api/v1/entities/templates/{}/publish'.format(tenant,entityTemplateId)
    headers = {'Authorization': token, 'Content-Type': 'application/json'}
    response = requests.post(url=url,headers=headers)
    return response

def createDMfromSnapshot(accessToken,tenant,snapshot):
    '''
    Main function to create and publish data models at a new tenant.
    :param accessToken: AccessToken new tenant
    :param tenant: New tenant domain
    :param snapshot: List of snapshot (json format) of each data model to be published. 
    :return: empty
    '''
    count = 0
    for snap in snapshot:
        count+=1
        #print(snap['label'])
        sent = sendDataModel(accessToken,tenant,snap)
        if sent.ok:
            publishDataModel(accessToken,sent.json()['mdmId'],tenant)
        else:
            print('Error')
            print(sent.text)


#lista = get_DataModels(accessToken_supply,'supplychain')
#createDMfromSnapshot(accessToken_schulz,'schulz',lista)



## Copy and create Named Queries

def get_GetNamedQueries(accessToken,tenant,filename='data/namedQueries.json'):
    '''
    Copy all named queries from a tenant
    :param token: AccessToken tenant
    :param tenant: tenant domain
    :param filename: file to save the named queries
    :return: list of named queries
    '''
    pageSize = '-1'
    url_filter = 'https://{}.carol.ai/api/v1/namedQueries?offset=0&pageSize={}&sortOrder=ASC'.format(tenant,pageSize)
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    response = requests.get(url=url_filter,headers=headers)
    if response.ok:
        response.encoding='utf-8'
        result = json.loads(response.text,encoding="utf-8")
        if (len(result['hits']) == result['totalHits']):
            print('{} NamedQueries received'.format(len(result['hits'])))
            result = result['hits']
            with open(filename, 'w',encoding='utf8') as outfile:
                json.dump(result, outfile, indent=2,ensure_ascii=False)
        else:
            print('{} NamedQueries received, expected = {}'.format(len(result['hits']),result['totalHits']))
    else:
        print('Error getting named queries:', response.text)
        return
    return result

def creatingNamedQueries(accessToken,tenant,namedQueries):
    '''
    Create named queries at the new tenant.
    :param token: AccessToken tenant
    :param tenant: tenant domain
    :param namedQueries: list of named queries to be sent
    :return: empty
    '''
    url_filter = 'https://{}.carol.ai/api/v1/namedQueries'.format(tenant)
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    for query in namedQueries:
        query.pop('mdmId',None)
        query.pop('mdmTenantId',None)
        response = requests.post(url=url_filter,headers=headers,json=query)
        if not response.ok:
            print('Error sending named query: {}'.format(response.text))
    print('Finished!')



def deleteNamedQueries(accessToken,tenant,namedQueries):
    '''
    Delete named query from a tenant
    :param accessToken: AccessToken
    :param tenant: tenant domain
    :param namedQueries: list of named queries to be deleted
    :return: 
    '''
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    names = [name['mdmQueryName'] for name in namedQueries]
    for name in names:
        url_filter = 'https://{}.carol.ai/api/v1/namedQueries/name/{}'.format(tenant,name)
        response = requests.delete(url=url_filter,headers=headers)
    print('Finished!')



#namedQueries = get_GetNamedQueries(accessToken_supply,subdomain_supply)
#creatingNamedQueries(accessToken_schulz,subdomain_schulz,namedQueries)


#POP revursive
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


#popKeys = ['mdmId', 'mdmLastUpdated', 'mdmCreated', 'mdmTenantId', ]

def getStaging(accessToken, tenant, staging_names, applicationId):
    '''
    Get staging table schema
    :param accessToken: AccessToken
    :param tenant: tenant domain
    :param staging_names: staging tables names
    :param applicationId: connector id
    :return: list of schemas for each staging table into a connector 
    '''
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    schema_list = []
    for name in staging_names:
        url_filter = 'https://{}.carol.ai/api/v1/staging/entities/types/{}/schema?applicationId={}'.format(tenant, name,
                                                                                                           applicationId)
        response = requests.get(url=url_filter, headers=headers)
        if response.ok:
            response.encoding = 'utf-8'
            schema_list.append(json.loads(response.text, encoding='utf8'))
        else:
            print('Error getting staging {}'.format(response.text))
            return

    return schema_list


def createConnectors(accessToken, tenant, data):
    '''
    Create a connector in a tenant from a list of connector of a second tenant
    :param accessToken: AccessToken
    :param tenant: tenant domain
    :param data: list of connector
    :return: request response
    '''
    url_connector = 'https://{}.carol.ai/api/v1/applications'.format(tenant)
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    data.pop('mdmId', None)
    data.pop('mdmTenantId', None)
    response = requests.post(url=url_connector, headers=headers, json=data)
    if response.ok:
        response.encoding = 'utf-8'
        return json.loads(response.text, encoding='utf-8')
    else:
        response = requests.get(url=url_connector, headers=headers)
        response.encoding = 'utf-8'
        response = json.loads(response.text, encoding='utf-8')
        response = response['hits']
        for i in response:
            if i['mdmName'] == data['mdmName']:
                conn_created = {}
                conn_created['mdmId'] = i['mdmId']
                return  conn_created




def createStagingTables(user, pw, appId, tenant, staging_table):
    '''
    Create a staging table in a conector
    :param user: tenant user name for authentication
    :param pw:  tenant password for authentication
    :param appId: connector id
    :param tenant: tenant domain
    :param staging_table: staging tables' schema to be created
    :return: list of new staging tables. 
    '''
    dropList = ['mdmCreated', 'mdmId', 'mdmLastUpdated', 'mdmTenantId', 'mdmStagingApplicationId']
    token = get_token(user, pw, tenant, appId=appId)
    accessToken = json.loads(token.text)['access_token']
    response_stag = []
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    for name in staging_table:
        scrub(name, dropList)
        type = name['mdmStagingType']
        url_connector = 'https://{}.carol.ai/api/v1/staging/entities/types/{}/schema'.format(tenant, type)
        response = requests.post(url=url_connector, headers=headers, json=name)

        if response.ok:
            response.encoding = 'utf-8'
            response_stag.append(json.loads(response.text, encoding='utf-8'))
        else:
            print('Error creating staging', response.text)
    return response_stag


def getMapping(accessToken, tenant, entityTemplateIds):
    '''
    get mapping of a staging table to a data model
    :param accessToken: tenant accessToken
    :param tenant: tenant domain
    :param entityTemplateIds: list of entity template ids 
    :return: list of mappings for each template id
    '''
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    all_mappings = []
    for entityTemplateId in entityTemplateIds:
        url_map = 'https://{}.carol.ai/api/v1/entities/templates/{}/published'.format(tenant, entityTemplateId)
        response = requests.get(url=url_map, headers=headers)
        if response.ok:
            response.encoding = 'utf-8'
            mapping = json.loads(response.text, encoding='utf-8')
            all_mappings.append(mapping)
        else:
            print('Error to retrieve mapping: {}'.format(response.text))
    return all_mappings


def getMappingSnapshot(accessToken, tenant, mdmEntityMappings):
    '''
    Get mapping snapshot to be used to create a new mapping rule in a second tenant
    :param accessToken: tenant accessToken
    :param tenant:  tenant domain
    :param mdmEntityMappings:  mdmEntityMappings
    :return: list of snapshot of mappings
    '''

    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    all_mappings = []
    for key, item in mdmEntityMappings.items():
        mdmApplicationId = mdmEntityMappings[key]['mdmApplicationId']
        entityMappingsId = mdmEntityMappings[key]['mdmId']
        mdmEntitySpace = mdmEntityMappings[key]['mdmEntitySpace']
        url_map = 'https://{}.carol.ai/api/v1/applications/{}/entityMappings/{}/snapshot?entitySpace={}&reverseMapping=false'. \
            format(tenant, mdmApplicationId, entityMappingsId, mdmEntitySpace)
        response = requests.get(url=url_map, headers=headers)
        if response.ok:
            response.encoding = 'utf-8'
            mapping = json.loads(response.text, encoding='utf-8')
            all_mappings.append(mapping)
        else:
            print('Error to retrieve mapping: {}'.format(response.text))
    return all_mappings


def publishMapping(accessToken, tenant, applicationId, entityMappingId):
    '''
    publish mapping
    :param accessToken: tenant accessToken
    :param tenant:  tenant domain
    :param applicationId: connector id
    :param entityMappingId: entityMappingId
    :return: request response
    '''
    url = 'https://{}.carol.ai/api/v1/applications/{}/entityMappings/{}/publish'.format(tenant, applicationId,
                                                                                        entityMappingId)
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    response = requests.post(url=url, headers=headers)
    return response


def createMapping(user, pw, appId, tenant, mappings):
    '''
    create and publish mappings from a list of mappings snapshots
    :param user: tenant user name for authentication
    :param pw:  tenant password for authentication
    :param appId: connector id
    :param tenant: tenant domain
    :param mappings: list of mappings' snapshots
    :return: 
    '''
    token = get_token(user, pw, tenant, appId=appId)
    accessToken = json.loads(token.text)['access_token']
    headers = {'Authorization': accessToken, 'Content-Type': 'application/json'}
    response_mapping = []
    for mapping in mappings:
        url_mapping = 'https://{}.carol.ai/api/v1/applications/{}/entityMappings/snapshot'.format(tenant, appId)
        response = requests.post(url=url_mapping, headers=headers, json=mapping)
        if response.ok:
            response.encoding = 'utf-8'
            response_mapping = json.loads(response.text, encoding='utf-8')
            entityMappingId = response_mapping['mdmEntityMapping']['mdmId']
            publishMapping(accessToken, tenant, appId, entityMappingId)
        else:
            print('Error creating staging', response.text)
    return response_mapping


def cloneConnectors(accessToken_from, tenant_from, accessToken_to, tenant_to, user, pw, filename='data/connectors.json'):
    '''
    copy all connectors, staging schemas and mapping from a source tenant to a new tenant. 
    :param accessToken_from: source tenant accessToken
    :param tenant_from: source tenant domain
    :param accessToken_to: new tenant accessToken
    :param tenant_to: new tenant domain
    :param user: new tenant user name for authentication
    :param pw:  new tenant password for authentication
    :param filename: json with the source tenant connectors
    :return: 
    '''
    pageSize = '-1'
    url_staging = 'https://{}.carol.ai/api/v1/applications?includeMappings=true&pageSize={}'.format(tenant_from,
                                                                                                    pageSize)
    headers = {'Authorization': accessToken_from, 'Content-Type': 'application/json'}
    response = requests.get(url=url_staging, headers=headers)
    if response.ok:
        response.encoding = 'utf-8'
        result = json.loads(response.text, encoding='utf-8')
        if (len(result['hits']) == result['totalHits']):
            print('{} Connectors received'.format(len(result['hits'])))
            result = result['hits']
            with open(filename, 'w', encoding='utf8') as outfile:
                json.dump(result, outfile, indent=2, ensure_ascii=False)
        else:
            print('{} Connectors received, expected = {}'.format(len(result['hits']), result['totalHits']))
            return
    else:
        print('Error requesting connectors', response.text)
    print('Getting Mapping')
    new_connector = []
    for connector in result:
        conn_created = createConnectors(accessToken_to, tenant_to, connector.copy())
        new_connector.append(conn_created)
        applicationId_new = conn_created['mdmId']
        if len(connector['mdmEntityMappings']) > 0:
            print('Creating Schema and Mappings')
            mdmMasterEntityName_list = list(connector['mdmEntityMappings'].keys())
            schema_list = getStaging(accessToken_from, tenant_from, mdmMasterEntityName_list, connector['mdmId'])

            ###  mdmMasterEntityId_list = [connector['mdmEntityMappings'][key]['mdmMasterEntityId'] for key in connector['mdmEntityMappings'].keys()]
            #### all_mappings = getMapping(accessToken_from,tenant_from,mdmMasterEntityId_list,applicationId_old)

            all_mappings = getMappingSnapshot(accessToken_from, tenant_from, connector['mdmEntityMappings'])

            response_stag = createStagingTables(user, pw, applicationId_new, tenant_to, schema_list)

            createMapping(user, pw, applicationId_new, tenant_to, all_mappings)

    return result, all_mappings, new_connector, schema_list, response_stag