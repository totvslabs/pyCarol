# pyCarol   <img src="https://github.com/TOTVS/carol-ds-retail/blob/master/recommender-project/image/pycarol.jpg" alt="Carol" width="32" data-canonical-src="http://svgur.com/i/ue.svg"> 

# Table of Contents
1. [APIs Implemented](#apis-implemented)
2. [Using pyCarol](#using-pyCarol)
   1. [Getting an access token](#getting-an-access-token)
   1. [Using API Key](#using-api-key)
   1. [Running on a local Machine](#running-on-a-local-machine)
   1. [Processing filter queries](#processing-filter-queries)
   1. [Processing named queries](#processing-named-queries)
   1. [Sending data](#sending-data)
3. [Carol Apps](#carol-apps)
4. [Cloning a tenant](#cloning-a-tenant)
5. [pyCarol utils](#pycarol-utils)



## APIs Implemented

This package implements some of Carol's APIs. The following endpoints are implemented: 
    
    - V2 OAuth2: (loginCarol.py)
        1. POST - /api/v2/oauth2/token
        2. POST/GET - /api/v2/oauth2/token/{access_token}
        
    - v2 ApiKey (loginCarol.py)
        1. POST - /api/v2/apiKey/details
        2. POST - /api/v2/apiKey/issue
        3. POST - /api/v2/apiKey/revoke
        
    - v2 Tenants (utils.py)
        1. GET - /api/v2/tenants/domain/{domain}
        
    - v2 Queries (queriesCarol.py)
        1. POST - /api/v2/queries/filter
        2. POST - /api/v2/queries/named/{query_name}
        2. DELETE - /api/v2/queries/filter 
        2. POST - /api/v2/queries/filter/{scrollId}
        
    - v2 Staging (stagingCarol.py)
        1. GET/POST - /api/v2/staging/tables/{table}
        2. GET-POST - /api/v2/staging/tables/{table}/schema
        
    - v1 Entity Mappings (entityMappingsCarol.py)
        1. GET/POST - /api/v1/connectors/{connectorId}/entityMappings
        2. GET/POST/DELETE - /api/v1/connectors/{connectorId}/entityMappings/{entityMappingId}
        3. PUT - /api/v1/connectors/{connectorId}/entityMappings/{entityMappingId}/snapshot
        4. POST - /api/v1/connectors/{connectorId}/entityMappings/snapshot
        
    - v1 Entity Template (entityTemplateCarol.py)
        1. GET - /api/api/v1/entities/templates/snapshot
        4. GET - /api/v1/entities/templates/name/{entityTemplateName}

    - v1 Connectors (connectorsCarol.py)
        1. GET/POST - /api/v1/connectors
        2. DELETE - /api/v1/connectors/{connectorId}
        3. GET - /api/v1/connectors/name/{connectorName}
        4. GET - /api/v1/connectors/{connectorId}/stats
        
    - v1 Tenant Apps (tenantAppsCarol)
        1. GET - /api/v1/tenantApps
        2. GET - /api/v1/tenantApps/{id}
        3. GET - /api/v1/tenantApps/name/{name}
        4. GET -/api/v1/tenantApps/{tenantAppId}/settings
        
 We also have a Schema Generator (schemaGenerator.py).

 ### Using pyCarol
 
The process always starts after we obtain an access token.

#####  Getting an access token


 All APIs need a login object. It creates/refreshes tokens.
```python
from pycarol import loginCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
token_object.newToken()
print('This is a valid access token {}'.format(token_object.access_token))
token_object.refreshToken()
print('This is refreshed access token {}'.format(token_object.access_token))
```  
#####  Running on a local Machine

If you are running the application on a local machine you need to enter the port you are using:

```python
from pycarol import loginCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
                                     
token_object.dev = ':8888'                                 
token_object.newToken()

```

##### Using API Key
To use API keys instead of username and password:

```python
from pycarol import loginCarol
token_object = loginCarol.loginCarol(X_Auth_Key =  X_Auth_Key,
                                     X_Auth_ConnectorId = X_Auth_ConnectorId,
                                     domain= domain )
```  

To generate an API key

```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
token_object.getAPIKey()
print('This is a API key {}'.format(token_object.X_Auth_Key))
token_object.refreshToken()
print('This is the connector Id {}'.format(token_object.X_Auth_ConnectorId))
```

To be able of getting the details of the API key you can do:

```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(X_Auth_Key =  X_Auth_Key,
                                     X_Auth_ConnectorId= X_Auth_ConnectorId)
                                     
token_object.getAPIKeyDetails()
token_object.APIKeyDetails
```

Finally, to revoke an API key:

```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
token_object.newToken()

token_object.revokeAPIKey(X_Auth_Key =  X_Auth_Key,
                          X_Auth_ConnectorId= X_Auth_ConnectorId)
```

##### Processing filter queries

```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)                            
token_object.newToken()

json_query = {
          "mustList": [
            {
              "mdmFilterType": "TYPE_FILTER",
              "mdmValue": "ratingsGolden"
            },
            {
              "mdmFilterType": "TERM_FILTER",
              "mdmKey": "mdmGoldenFieldAndValues.userid",
              "mdmValue": 406
            }
          ]
        }
        
query_response = queriesCarol.queryCarol(token_object)
#To get all records returned:
query_response.newQuery(json_query= json_query, max_hits = float('inf'),only_hits=True)
#To get only 1000
query_response.newQuery(json_query= json_query, max_hits = 1000, only_hits=True)
#the response is here
query_response.query_data
#If I want to save the response 
query_response.newQuery(json_query= json_query, max_hits = 1000, only_hits=True, save_results = True,
                        filename = 'PATH/response.json')
```  
The parameter `only_hits = True` will make sure that the only records into the path `$hits.mdmGoldenFieldAndValues`.
 If you want all the response use `only_hits = False`. Also, if your filter has aggregation, you should use 
 `only_hits = False`. 


##### Processing named queries

```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)                           
token_object.newToken()

named_query = 'revenueHist'  # named query name
payload = {"bin":"1d","cnpj":"24386434000130"}  #payload to send.
named_query_resp = queriesCarol.queryCarol(token_object)
#To get all records returned:
named_query_resp.namedQuery(named_query = named_query, json_query = payload)
#the response is here
named_query_resp.query_data

```
It is possible to use all the parameters used in the filter query, i.e., `only_hits` , `save_results`, etc.
For more information for the possible input parameters check the docstring.

What if one does not remember the parameters for a given named query?


```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)                             
token_object.newToken()

named_query = 'revenueHist'  # named query name
named_query_resp = queriesCarol.queryCarol(token_object)
named_query_resp.namedQueryParams(named_query = named_query)
> {'revenueHist': ['*cnpj', 'dateFrom', 'dateTo', '*bin']}  #Parameters starting by * are mandatory. 

```
 
 ##### Sending data
 
 The first step to send data to Carol is to create a connector. 
 
 ```python
from pycarol import loginCarol, connectorsCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)                           
token_object.newToken()

conn = connectorsCarol.connectorsCarol(token_object)
conn.createConnector(connectorName = 'my_conector', connectorLabel = "conector_label", groupName = "GroupName")
connectorId = conn.connectorId  # this is the just created connector Id

```
With the connector Id on hands we  can create the staging schema and then create the staging table. Assuming we have 
a sample of the data we want to send. 

  ```python
from pycarol import stagingCarol

json_ex = {"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'}   }
schema = stagingCarol.stagingSchema(token_object)
schema.createSchema(fields_dict = json_ex, mdmStagingType='my_stag', mdmFlexible='false',
                       crosswalkname= 'my_crosswalk' ,crosswalkList=['name'])
                       
#sending schaema
schema.sendSchema(connectorId=connectorId)  #here connectorId is that one created above
```

The json schema will be in the variable `schema.schema`. The code above will create the following schema:
```python
{
  'mdmCrosswalkTemplate': {
    'mdmCrossreference': {
      'my_crosswalk': [
        'name'
      ]
    }
  },
  'mdmFlexible': 'false',
  'mdmStagingMapping': {
    'properties': {
      'email': {
        'properties': {
          'email': {
            'type': 'string'
          },
          'type': {
            'type': 'string'
          }
        },
        'type': 'nested'
      },
      'name': {
        'type': 'string'
      }
    }
  },
  'mdmStagingType': 'my_stag'
}
```
To send the data  (assuming we have a json with the data we want to send). 

  ```python
from pycarol import stagingCarol

json_ex = [{"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'}   },
           {"name":'Leandro',"email": {"type": "email", "email": 'Leandro@totvs.com.br'}   },
           {"name":'Mario',"email": {"type": "email", "email": 'mario@totvs.com.br'}   },
           {"name":'Marcelo',"email": {"type": "email", "email": 'marcelo@totvs.com.br'}   }]
           
           
send_data = stagingCarol.sendDataCarol(token_object)
send_data.sendData(stagingName = 'my_stag', data = json_ex, step_size = 2, 
                   connectorId=connectorId, print_stats = True)
                       
```
The parameter `step_size` says how many registers will be sent each time. Remember the the max size per payload is 
5MB. The parameter  `data` can be a pandas DataFrame (Beta).

OBS: It is not possible to create a mapping using pycarol. The Mapping has to be done via the UI



### Carol Apps
We can use pyCarol to access the settings of a Carol App.  
  ```python
from pycarol import loginCarol, tenantAppsCarol
token = loginCarol.loginCarol(username= username_from, password=my_password_from, 
                                     domain = my_domain_from, connectorId=my_connectorId_from)
token.newToken()
app = tenantAppsCarol.tenantApps(token)
app.getSettings(appName='studentRetention')
print(app.appSettings)
> {'subsidiary':'1n','activeenrollment':'2n','finishedenrollment':'3n',
     'cancelledenrollment':'4','maximumperiod':'5n','defaultlimit':'6n',
     'relattendances':'7n','relevantmajors':'8n','trigger':'9n'}
```
The settings will be returned as a dictionary where the keys are the parameter names and the values are the value for 
for that parameter. We can access the full settings as a dictionary through the variable `app.fullSettings`, where the keys
are the parameter names and the values are the full responses for that parameter. 
