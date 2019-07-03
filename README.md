# pyCarol   <img src="https://github.com/TOTVS/carol-ds-retail/blob/master/recommender-project/image/pycarol.jpg" alt="Carol" width="32" data-canonical-src="http://svgur.com/i/ue.svg">

[![Build status](https://badge.buildkite.com/b92ca1611add8d61063f61c92b9798fe81e859d468aae36463.svg)](https://buildkite.com/totvslabs/pycarol)

# Table of Contents
1. [Initializing pyCarol](#using-pyCarol)
   1. [Using API Key](#using-api-key)
   1. [Running on a local Machine](#running-on-a-local-machine)
1. [Queries](#queries)
   1. [Filter queries](#processing-filter-queries)
   1. [Named queries](#processing-named-queries)
1. [Sending data](#sending-data)
1. [Logging](#logging)
1. [Settings](#settings)
1. [Data Validation](#validation)
2. [APIs Implemented](#apis-implemented)

## Initializing pyCarol

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
####  Running on a local Machine

If you are running the application on a local machine you need to enter the port you are using:

```python
from pycarol.auth.PwdAuth import PwdAuth
from pycarol.carol import Carol

carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
              auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTOR,
              port=8888)

```

#### Using API Key
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
details = carol.api_key_details(APIKEY,CONNECTORID)
```

Finally, to revoke an API key:

```python
carol.api_key_revoke(CONNECTORID)
```

## Queries

#### Filter queries

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
from pycarol.query import Query

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
Tasks(carol).info('Hello world!')
Tasks(carol).warn('Warning! Missing xyz')
Tasks(carol).error('Cannot connect to ABC, aborting')
```
These methods will use the current long task id provided by Carol when running your application.
For local environments you need to set that manually first on the beginning of your code:
```python
Tasks(carol).create('MyApp', 'TaskGroup').set_as_current_task()
```

## Settings
We can use pyCarol to access the settings of your Carol App.
```python
# To get a specific setting
Settings(carol).get('setting_name')

# To get all the settings
print(Settings(carol).all())
```
Using the all(), the settings will be returned as a dictionary where the keys are the parameter names and the values are
the value for that parameter. To access the full settings as a dictionary, use the all_full() method, where the keys
are the parameter names and the values are the full responses for that parameter.

Please note that your app must be created in Carol and its name be correctly setup during pyCarol initialization

## Data Validation
There are some built-in data validation in pyCarol that we can use to ensure the data is ok
```python
from pycarol.validator import Validator

validator = Validator(carol)
# To check that the field code on the data model products is at least 80% filled
validator.assert_non_empty(data_model='products', field='code', threshold=0.8)

# Or if we already have the datamodel loaded in a variable
validator.assert_non_empty(data=data_model, field='code', threshold=0.8)

# To check if there a minimum number of records in a data model:
validator.assert_min_quantity(data_model='products', min_qty=1000)

# Custom threshold validation
value = some_calculation
validator.assert_custom('MyValidation', value, min_req_value)

# And at the end of the validations, to post all validation issues to Carol as a long task log:
validator.post_results()
```

