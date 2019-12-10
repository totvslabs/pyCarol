# pyCarol   <img src="https://github.com/TOTVS/carol-ds-retail/blob/master/recommender-project/image/pycarol.jpg" alt="Carol" width="32" data-canonical-src="http://svgur.com/i/ue.svg">

[![Build status](https://badge.buildkite.com/b92ca1611add8d61063f61c92b9798fe81e859d468aae36463.svg)](https://buildkite.com/totvslabs/pycarol)

# Table of Contents
1. [Initializing pyCarol](#using-pyCarol)
   1. [Using API Key](#using-api-key)
   1. [Good practice using token](#good-practice-using-token)
1. [Queries](#queries)
   1. [Filter queries](#processing-filter-queries)
   1. [Named queries](#processing-named-queries)
1. [Sending data](#sending-data)
1. [Logging](#logging)
1. [Settings](#settings)

## Initializing pyCarol

Carol is the main object to access pyCarol and all Carol's APIs.
```python
from pycarol import PwdAuth, Carol

carol = Carol(domain=TENANT_NAME, app_name=APP_NAME, 
              auth=PwdAuth(USERNAME, PASSWORD), organization=ORGANIZATION)
```
where `domain` is the tenant name, `app_name` is the Carol's app name, if any, `auth`
is the authentication method to be used (using user/password in this case) and `organization` is the organization 
one wants to connect. Carols's URL is build as www.ORGANIZATION.carol.ai/TENANT_NAME

#### Using API Key
To use API keys instead of username and password:

```python
from pycarol import ApiKeyAuth, Carol

carol = Carol(domain=DOMAIN,
              app_name=APP_NAME,
              auth=ApiKeyAuth(api_key=X_AUTH_KEY),
              connector_id=CONNECTOR, organization=ORGANIZATION)

```
In this case one changes the authentication method to `ApiKeyAuth`. Noticed that one needs to pass the `connector_id`
too. An API key is always associated to a connector ID. 

It is possible to use pyCarol to generate an API key

```python
from pycarol import PwdAuth, ApiKeyAuth, Carol

carol = Carol(domain=TENANT_NAME, app_name=APP_NAME, organization=ORGANIZATION,
              auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTOR)
api_key = carol.issue_api_key()

print(f"This is a API key {api_key['X-Auth-Key']}")
print(f"This is the connector Id {api_key['X-Auth-ConnectorId']}")
```

To get the details of the API key you can do:

```python
details = carol.api_key_details(APIKEY, CONNECTORID)
```

Finally, to revoke an API key:

```python
carol.api_key_revoke(CONNECTORID)
```


#### Good practice using token
Never write in plain text your password/API token in your application. Use environment variables. pyCarol can use 
environment variables automatically. When none parameter is passed to the Carol constructor pycarol will look for:

 1. `CAROLTENANT` for domain
 2. `CAROLAPPNAME` for app_name
 3. `CAROL_DOMAIN` for environment
 4. `CAROLORGANIZATION` for organization
 4. `CAROLAPPOAUTH` for auth
 5. `CAROLCONNECTORID` for connector_id
 6. `CAROLUSER` for carol user email
 7. `CAROLPWD` for user password.  
 
 e.g., one can create a `.env` file like this:
 
 ```.env
CAROLAPPNAME=myApp
CAROLTENANT=myTenant
CAROLORGANIZATION=myOrganization
CAROLAPPOAUTH=myAPIKey
CAROLCONNECTORID=myConnector
```
 and then
 
 ```python
from pycarol import Carol
from dotenv import load_dotenv
load_dotenv(".env") #this will import these env variables to your execution. 
carol = Carol()
```

## Queries

#### Filter queries

```python
from pycarol.filter import TYPE_FILTER, TERM_FILTER, Filter
from pycarol import Query
json_query = Filter.Builder() \
    .must(TYPE_FILTER(value='ratings' + "Golden")) \
    .must(TERM_FILTER(key='mdmGoldenFieldAndValues.userid.raw',value='123'))\
    .build().to_json()

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
from pycarol.query import Query
from pycarol.filter import TYPE_FILTER, Filter, CARDINALITY

json_query = Filter.Builder() \
    .must(TYPE_FILTER(value='datamodelname' + "Golden")) \
    .aggregation(CARDINALITY(name='cardinality', params = ["mdmGoldenFieldAndValues.taxid.raw"], size=40))\
    .build().to_json()

query = Query(carol, get_aggs=True, only_hits=False)
query.query(json_query).go()
query.results

```


#### Named queries

```python
from pycarol.query import Query

named_query = 'revenueHist'  # named query name
params = {"bin":"1d","cnpj":"24386434000130"}  #query parameters to send.
results = Query(carol).named(named_query, params=params).go().results
```
It is possible to use all the parameters used in the filter query, i.e., `only_hits` , `save_results`, etc.
For more information for the possible input parameters check the docstring.

What if one does not remember the parameters for a given named query?


```python
named_query = 'revenueHist'  # named query name
Query(carol).named_query_params(named_query)
> {'revenueHist': ['*cnpj', 'dateFrom', 'dateTo', '*bin']}  #Parameters starting by * are mandatory.

```

## Sending data

 The first step to send data to Carol is to create a connector.

 ```python
conn = Connectors(carol).create(connectorName='my_conector', connectorLabel="conector_label", groupName="GroupName")
connectorId = conn.connectorId  # this is the just created connector Id

```
With the connector Id on hands we can create the staging schema and then create the staging table. Assuming we have
a sample of the data we want to send.

  ```python
from pycarol.staging import Staging

json_ex = {"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'} }

staging = Staging(carol)
staging.create_schema(staging_name='my_stag', fields_dict = json_ex, mdm_flexible='false',
                      crosswalk_name= 'my_crosswalk' ,crosswalk_list=['name'], connector_id=connector_id)
#here connector_id is that one created above or, if not specified, the one set during pyCarol initialization
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
from pycarol.staging import Staging

json_ex = [{"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'}   },
           {"name":'Leandro',"email": {"type": "email", "email": 'Leandro@totvs.com.br'}   },
           {"name":'Joao',"email": {"type": "email", "email": 'joao@rolima.com.br'}   },
           {"name":'Marcelo',"email": {"type": "email", "email": 'marcelo@totvs.com.br'}   }]


staging = Staging(carol)
staging.sendData(staging_name = 'my_stag', data = json_ex, step_size = 2,
                 connector_id=connectorId, print_stats = True)
```
The parameter `step_size` says how many registers will be sent each time. Remember the the max size per payload is
5MB. The parameter  `data` can be a pandas DataFrame (Beta).

OBS: It is not possible to create a mapping using pycarol. The Mapping has to be done via the UI



## Logging
To log messages to Carol:
```python
from pycarol import Carol, CarolHandler
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
carol = CarolHandler(Carol())
carol.setLevel(logging.INFO)
logger.addHandler(carol)

logger.debug('This is a debug message') #This will not be logged in Carol. Level is set to INFO
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')
```
These methods will use the current long task id provided by Carol when running your application.
For local environments you need to set that manually first on the beginning of your code:
```python
import os
os.environ['LONGTASKID'] = task_id
```
We recommend to log only INFO+ information in Carol. If no TASK ID is passed it works as a Console Handler. 

## Settings
We can use pyCarol to access the settings of your Carol App.
```python

from pycarol.apps import Apps
app = Apps(carol)
settings = app.get_settings(app_name='my_app')
print(settings)

```
The settings will be returned as a dictionary where the keys are the parameter names and the values are
the value for that parameter. Please note that your app must be created in Carol.


