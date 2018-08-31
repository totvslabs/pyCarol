# pyCarol   <img src="https://github.com/TOTVS/carol-ds-retail/blob/master/recommender-project/image/pycarol.jpg" alt="Carol" width="32" data-canonical-src="http://svgur.com/i/ue.svg"> 

# Table of Contents
1. [APIs Implemented](#apis-implemented)
2. [Using pyCarol](#using-pyCarol)
   1. [Initializing pyCarol](#getting-an-access-token)
   1. [Using API Key](#using-api-key)
   1. [Running on a local Machine](#running-on-a-local-machine)
   1. [Processing filter queries](#processing-filter-queries)
   1. [Processing named queries](#processing-named-queries)
   1. [Sending data](#sending-data)
   1. [Logging](#logging)
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

##### Initializing pyCarol


Carol is the main object to access pyCarol and all APIs need it.
```python
from pycarol.auth.PwdAuth import PwdAuth
from pycarol.carol import Carol

carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
              auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTOR)
              
```  
where `domain` is the tenant name, `app_name` is the app name one is using to access, if any, 
is the authentication method to be used (using user/password in this case) and `connector_id` is the connector 
one wants to connect.
#####  Running on a local Machine

If you are running the application on a local machine you need to enter the port you are using:

```python
from pycarol.auth.PwdAuth import PwdAuth
from pycarol.carol import Carol

carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
              auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTOR,
              port=8888)

```

##### Using API Key
To use API keys instead of username and password:

```python
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol

carol = Carol(domain=DOMAIN, 
              app_name=APP_NAME, 
              auth=ApiKeyAuth(api_key=X_AUTH_KEY),
              connector_id=CONNECTOR)

```  

To generate an API key

```python
from pycarol.auth.PwdAuth import PwdAuth
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol

api_key = Carol(domain=TENANT_NAME, app_name=APP_NAME,
              auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTOR).issue_api_key()


print(f"This is a API key {api_key['X-Auth-Key']}")
print(f"This is the connector Id {api_key['X-Auth-ConnectorId']}")
```

To be able of getting the details of the API key you can do:

```python
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol

carol = Carol(domain=DOMAIN, 
              app_name=APP_NAME, 
              auth=ApiKeyAuth(api_key=X_AUTH_KEY),
              connector_id=CONNECTOR)
              
details = carol.api_key_details(APIKEY,CONNECTORID)
```

Finally, to revoke an API key:

```python
carol = Carol(domain=DOMAIN, 
              app_name=APP_NAME, 
              auth=ApiKeyAuth(api_key=X_AUTH_KEY),
              connector_id=CONNECTOR)
              
carol.api_key_revoke(CONNECTORID)
```

##### Processing filter queries

```python
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


FIELDS_ITEMS = ['mdmGoldenFieldAndValues.mdmaddress.coordinates']
query = Query(carol, page_size=10, print_status=True, only_hits=True,
              fields=FIELDS_ITEMS, max_hits=200).query(json_query).go()
query.results

```  
The result will be `200` hits of the query `json_query`  above, the pagination will be 10, that means in each response
there will be 10 records. The query will return only the fields set in `FIELDS_ITEMS`. 
The parameter `only_hits = True` will make sure that only records into the path `$hits.mdmGoldenFieldAndValues` will return.
 If one wants all the response use `only_hits = False`. Also, if your filter has an aggregation, one should use 
 `only_hits = False` and `get_aggs=True`, e.g.,  
 
 
```python
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol
from pycarol.query import Query

carol = Carol(domain=DOMAIN, 
              app_name=APP_NAME, 
              auth=ApiKeyAuth(api_key=X_AUTH_KEY),
              connector_id=CONNECTOR)

jsons = {
  "mustList": [
    {
      "mdmFilterType": "TYPE_FILTER",
      "mdmValue": "datamodelGolden"
    }
  ],
  "aggregationList": [
    {
      "type": "CARDINALITY",
      "name": "campaign",
      "params": [
        f"mdmGoldenFieldAndValues.taxid"
      ]
    }
  ]
}


query = Query(carol, get_aggs=True, only_hits=False,page_size=0)
query.query(jsons).go()
query.results

``` 


##### Processing named queries

```python
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.carol import Carol
from pycarol.query import Query

carol = Carol(domain=DOMAIN,
              app_name=APP_NAME,
              auth=ApiKeyAuth(api_key=X_AUTH_KEY),
              connector_id=CONNECTOR)


named_query = 'revenueHist'  # named query name
params = {"bin":"1d","cnpj":"24386434000130"}  #query parameters to send.
results = Query(carol).named(named_query, params=params).go().results
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
With the connector Id on hands we can create the staging schema and then create the staging table. Assuming we have
a sample of the data we want to send. 

  ```python
from pycarol import stagingCarol

json_ex = {"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'}   }
schema = stagingCarol.stagingSchema(token_object)
schema.createSchema(fields_dict = json_ex, mdmStagingType='my_stag', mdmFlexible='false',
                       crosswalkname= 'my_crosswalk' ,crosswalkList=['name'])

#sending schema
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
           {"name":'Joao',"email": {"type": "email", "email": 'joao@rolima.com.br'}   },
           {"name":'Marcelo',"email": {"type": "email", "email": 'marcelo@totvs.com.br'}   }]
           
           
send_data = stagingCarol.sendDataCarol(token_object)
send_data.sendData(stagingName = 'my_stag', data = json_ex, step_size = 2, 
                   connectorId=connectorId, print_stats = True)
                       
```
The parameter `step_size` says how many registers will be sent each time. Remember the the max size per payload is 
5MB. The parameter  `data` can be a pandas DataFrame (Beta).

OBS: It is not possible to create a mapping using pycarol. The Mapping has to be done via the UI



 ##### Logging
To log messages to Carol:
```python
Tasks(carol).info('Hello world!')
Tasks(carol).warn('Warning! Missing xyz')
Tasks(carol).error('Cannot connect to ABC, aborting')
```
These methods will use the current long task id provided by Carol when running your application.
For local environments you need to set that manually first on the beginning of your code:
```python
Tasks(carol).create('MyApp', 'TaskGroup').set_as_current_task()
```


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
