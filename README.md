<h1 align="center">PyCarol</h1>

<hr/>

PyCarol is a Python SDK designed to support data ingestion and data access workflows on Carol.
It provides abstractions for authentication, connector and staging management, data ingestion, and querying, enabling reliable integration with Carol services using Python.
The SDK encapsulates low-level API communication and authentication logic, making data pipelines easier to build, maintain, and operate.

## Table of Contents

* [Getting Started](#getting-started)
* [Recommended authentication method](#recommended-authentication-method)
* [Explicit authentication methods](#explicit-authentication-methods)
  * [1. Using user/password](#1-using-userpassword)
  * [2. Using Tokens](#2-using-tokens)
  * [3. Using API Key](#3-using-api-key)
* [Setting up Carol entities](#setting-up-carol-entities)
* [Sending Data](#sending-data)
* [Reading data](#reading-data)
* [Carol In Memory](#carol-in-memory)
* [Logging](#logging)
  * [Prerequisites](#prerequisites)
  * [Logging messages to Carol](#logging-messages-to-carol)
  * [Notes](#notes)
* [Calling Carol APIs](#calling-carol-apis)
* [Settings](#settings)
* [Useful Functions](#useful-functions)
* [Release process](#release-process)

## Getting Started

Run `pip install pycarol` to install the latest stable version from [PyPI](https://pypi.org/project/pycarol/).
Documentation for the [latest release](https://pycarol.readthedocs.io/en/2.55.0/) is hosted on [Read the Docs](https://pycarol.readthedocs.io/).

This will install the minimal dependencies. To install pyCarol with the `dataframes` dependencies use
``pip install pycarol[dataframe]``, or to install with dask+pipeline dependencies use ``pip install pycarol[pipeline,dask]``

The options we have are: `complete`, `dataframe`, `onlineapp`, `dask`, `pipeline`

To install from source:

1. ``pip install -r requirements.txt`` to install the minimal requirements;
2. ``pip install -e . ".[dev]"`` to install the minimal requirements + dev libs;
3. ``pip install -e . ".[pipeline]"`` to install the minimal requirements + pipelines dependencies;
4. ``pip install -e . ".[complete]"`` to install all dependencies;


## Recommended authentication method

Never write in plain text your password/API token in your application. Use environment variables. pyCarol can use 
environment variables automatically. When none parameter is passed to the Carol constructor pycarol will look for:

 1. ``CAROLTENANT`` for domain or tenant name
 2. ``CAROLAPPNAME`` for app_name
 3. ``CAROL_DOMAIN`` for environment or tenant name
 4. ``CAROLORGANIZATION`` for organization
 5. ``CAROLAPPOAUTH`` for auth
 6. ``CAROLCONNECTORID`` for connector_id
 7. ``CAROLUSER`` for carol user email
 8. ``CAROLPWD`` for user password
 
Carols's URL is build as ``www.ORGANIZATION.carol.ai/TENANT_NAME``

For example, one can create a .env file like this:

```python
    CAROLAPPNAME=myApp
    CAROLTENANT=myTenant
    CAROLORGANIZATION=myOrganization
    CAROLAPPOAUTH=myAPIKey
    CAROLCONNECTORID=myConnector
```
Use this code to read the .env file created:

```python

    from pycarol import Carol
    from dotenv import load_dotenv
    load_dotenv(".env") #this will import the env variables to your execution.
    carol = Carol()
```

## Explicit authentication methods

Carol is the main object to access pyCarol and all Carol's APIs. There are different ways to initialize it.

#### 1. Using user/password

```python

    from pycarol import PwdAuth, Carol
    carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
                  auth=PwdAuth(USERNAME, PASSWORD), organization=ORGANIZATION)
```
``domain`` is the tenant name
``app_name`` is the Carol's app name, if any
``auth`` authentication method to be used (using user/password in this case)
``organization`` is the organization one wants to connect. 

#### 2. Using Tokens

It is also possible to initialize the Carol object using a Oauth2 token generated from a username and password.
This is useful for applications that need to temporary authenticate programmatically without storing user credentials.

```python

    from pycarol import PwdKeyAuth, Carol
    carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
                  auth=PwdKeyAuth(pwd_auth_token), organization=ORGANIZATION)
```

In case the Carol Organization you want to login uses TOTVS Fluig Identity, you can use `PwdFluig` and manually provide an access_token.

```python

    from pycarol import PwdFluig, Carol
    carol = Carol(domain=TENANT_NAME,
                auth=PwdFluig(USER_LOGIN), organization=ORGANIZATION)
```

You will be asked an access_token once. After providing correct access_token, pyCarol will manage authorization until your refresh_token expires.

#### 3. Using API Key

API keys provide an alternative authentication method to username and password.
An API key is always associated with a connector, so a valid connector_id is required.

- Generating an API key

Before using an API key, it must be generated.
This can be done using a user/password authentication.

```python

    from pycarol import PwdAuth, ApiKeyAuth, Carol

    carol = Carol(domain=TENANT_NAME, app_name=APP_NAME, organization=ORGANIZATION,
                  auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTORID)
    api_key = carol.issue_api_key()

    print(f"This is a API key {api_key['X-Auth-Key']}")
    print(f"This is the connector Id {api_key['X-Auth-ConnectorId']}")
```

- Authenticating using an API key

Once the API key has been generated, it can be used to authenticate requests by switching the authentication method to
ApiKeyAuth.

```python

    from pycarol import ApiKeyAuth, Carol

    carol = Carol(domain=DOMAIN,
                  app_name=APP_NAME,
                  auth=ApiKeyAuth(api_key=X_AUTH_KEY),
                  connector_id=CONNECTORID, organization=ORGANIZATION)
```

- Retrieving API key details

```python

    details = carol.api_key_details(APIKEY, CONNECTORID)
```

- Revoking an API key

```python

    carol.api_key_revoke(CONNECTORID)
```

## Setting up Carol entities

Before sending data to Carol, it is necessary to set up the required entities that define how and where data will be stored.
These entities establish the structural context for ingestion and must be created only once per dataset.

The setup process involves:

#### 1. Creating a connector, which represents the data source.

```python

    from pycarol import Connectors
    connector_id = Connectors(carol).create(name='my_connector', label="connector_label", group_name="GroupName")
    print(f"This is the connector id: {connector_id}")
```
#### 2. Creating a staging schema and table, which define the structure of the incoming data.

With the connector Id on hands we can create the staging schema and then create the staging table. 

```python

    from pycarol import Staging

    json_ex = {"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'} }

    staging = Staging(carol)
    staging.create_schema(staging_name='my_stag', data = json_ex,
                          crosswalk_name= 'my_crosswalk' ,crosswalk_list=['name'],
                            connector_name='my_connector')
```
The json schema will be in the variable ``schema.schema``. The code above will create the following schema:

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

## Sending Data

Once the connector and staging schema have been set up, data files can now be sent to Carol.

```python

    from pycarol import Staging

    json_ex = [{"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'}   },
               {"name":'Leandro',"email": {"type": "email", "email": 'Leandro@totvs.com.br'}   },
               {"name":'Joao',"email": {"type": "email", "email": 'joao@rolima.com.br'}   },
               {"name":'Marcelo',"email": {"type": "email", "email": 'marcelo@totvs.com.br'}   }]


    staging = Staging(carol)
    staging.send_data(staging_name = 'my_stag', data = json_ex, step_size = 2,
                     connector_id=CONNECTORID, print_stats = True)
```
The parameter ``step_size`` says how many registers will be sent each time. Remember the the max size 
per payload is 5MB. Pycarol uses default step_size parameter, so it becomes optional on `send_data` method.

The parameter  ``data`` can be a dict object or a pandas DataFrame. 

## Reading data

### From Staging Tables and Data Models (BigQuery layer)

#### 1. Read a subset of records and columns, with optional data transformations.

```python

    from pycarol import BQ, Carol

    bq = BQ(Carol())
    query_str = "SELECT * FROM stg_connectorname_tablename"
    results = bq.query(query_str)
```
#### 2. If a service account with BigQuery access is required, use the following code:

```python

    from pycarol import Carol
    from pycarol.bigquery import TokenManager

    tm = TokenManager(Carol())
    service_account = tm.get_token().service_account
```

After each execution of ``BQ.query``, the ``BQ`` object will have an attribute called
``job``. This attribute is of type ``bigquery.job.query.QueryJob`` and may be useful for
monitoring/debug jobs.

PyCarol provides access to BigQuery Storage API also. It allows for much faster reading
times, but with limited querying capabilities. For instance, only tables are readable,
so 'ingestion_stg_model_deep_audit' is ok, but 'stg_model_deep_audit' is not (it is a 
view). To compensate for limited querying capabilities, PyCarol implements `Carol In Memory`.

```python

    from pycarol import BQStorage, Carol

    bq = BQStorage(Carol())
    table_name = "ingestion_stg_model_deep_audit"
    col_names = ["request_id", "version"]
    restriction = "branch = '01'"
    sample_size = 1000
    df = bq.query(
        table_name,
        col_names,
        row_restriction=restriction,
        sample_percentage=sample_size,
        return_dataframe=True
    )
```
<details>
<summary>Legacy Reading Data Methods</summary>

### From Data Models (RT Layer): Filter queries

⚠️ **Deprecated**: This section is kept for backward compatibility and should not be used in new implementations.

Use this when you need low latency (only if RT layer is enabled).

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


The result will be ``200`` hits of the query ``json_query``  above, the pagination will be 10, that means in each response
there will be 10 records. The query will return only the fields set in ``FIELDS_ITEMS``.

The parameter ``only_hits = True`` will make sure that only records into the path ``$hits.mdmGoldenFieldAndValues`` will return.
If one wants all the response use ``only_hits = False``. Also, if your filter has an aggregation, one should use
``only_hits = False`` and ``get_aggs=True``, e.g.,


```python

    from pycarol import Query
    from pycarol.filter import TYPE_FILTER, Filter, CARDINALITY

    json_query = Filter.Builder() \
        .must(TYPE_FILTER(value='datamodelname' + "Golden")) \
        .aggregation(CARDINALITY(name='cardinality', params = ["mdmGoldenFieldAndValues.taxid.raw"], size=40))\
        .build().to_json()

    query = Query(carol, get_aggs=True, only_hits=False)
    query.query(json_query).go()
    query.results
```

### From Data Models (RT Layer): Named queries

```python

    from pycarol import Query

    named_query = 'revenueHist'  # named query name
    params = {"bin":"1d","cnpj":"24386434000130"}  #query parameters to send.
    results = Query(carol).named(named_query, params=params).go().results
```
It is possible to use all the parameters used in the filter query, i.e., ``only_hits`` ,
``save_results``, etc. For more information for the possible input parameters check the docstring.

What if one does not remember the parameters for a given named query?


```python

    named_query = 'revenueHist'  # named query name
    Query(carol).named_query_params(named_query)
    > {'revenueHist': ['*cnpj', 'dateFrom', 'dateTo', '*bin']}  #Parameters starting by * are mandatory.
```
</details>


## Carol In Memory

PyCarol provides an easy way to work with in-memory data using the Memory class, built on top of DuckDB. 
Queries are executed locally over in-memory data, without triggering BigQuery jobs or consuming BigQuery 
slots, and results are returned as pandas DataFrames. The recommended usage is with `BQStorage` objects.

```python

    from pycarol import Carol, PwdAuth, Memory, BQStorage, BQ
    from dotenv import load_dotenv

    load_dotenv()

    carol = Carol()

    storage = BQStorage(carol)
    memory = Memory()

    t = storage.query(
    'ingestion_stg_connectorname_tablename',
    column_names=['tenantid', 'processing', '_ingestionDatetime'],
    max_stream_count=50
    )

    memory.add("my_table", t)

    table = memory.query('''
        SELECT * FROM my_table 
        ''')

    print(table)
```

The sintax of Carol In Memory follows DuckDB [SQL Sintax](https://duckdb.org/docs/stable/sql/introduction)

## Logging

Logs are an essential tool for monitoring execution, troubleshooting errors, and understanding how data flows through Carol processes.
They provide visibility into application behavior and are especially useful during development and debugging.

##### Prerequisites

When running applications inside Carol, logs are automatically associated with the current long task.
For local execution, the task identifier must be set manually before configuring the logger.

```python

    import os
    os.environ['LONGTASKID'] = task_id
```
##### Logging messages to Carol

To send log messages to Carol, configure the Python logger using the ``CarolHandler``.

```python

    from pycarol import Carol, CarolHandler
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    carol_handler = CarolHandler(Carol())
    carol_handler.setLevel(logging.INFO)

    logger.addHandler(carol_handler)

    logger.debug('This is a debug message')  # Not logged in Carol (level is set to INFO)
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')
    logger.critical('This is a critical message')
```
##### Notes

- Log messages are associated with the current long task when running inside Carol.
- If no task ID is provided, the handler behaves as a console logger.
- It is recommended to log only ``INFO`` level and above in Carol to avoid excessive noise.


## Calling Carol APIs 

In addition to the high-level abstractions provided by pyCarol, it is also possible to call Carol APIs directly when needed.
This is useful for endpoints that are not yet covered by specific SDK methods.

```python

carol.call_api(
    'v1/tenantApps/subscribe/carolApps/{carol_app_id}',
    method='POST'
)
```

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


## Useful Functions

This section presents utility functions that provide additional helpers for common tasks when working with pyCarol.

#### 1. ``track_tasks``: Track a list of tasks.

```python

    from pycarol import Carol
    from pycarol.functions import track_tasks
    carol = Carol()
    def callback(task_list):
      print(task_list)
    track_tasks(carol=carol, task_list=['task_id_1', 'task_id_2'], callback=callback)
```

Release process
----------------
1. Open a PR with your change for `main` branch;
2. Once approved, merge into `main`;
3. In case there are any changes to the default release notes, please update them.
4. If any features are added or modified, please update README accordingly.

<hr/>

<p align="center">Made with ❤ at TOTVS IDeIA</p>