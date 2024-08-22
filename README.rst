=======
PyCarol
=======

.. image:: https://badge.buildkite.com/b92ca1611add8d61063f61c92b9798fe81e859d468aae36463.svg
    :target: https://buildkite.com/totvslabs/pycarol

.. contents::

Getting Started
---------------
Run ``pip install pycarol`` to install the latest stable version from `PyPI
<https://pypi.python.org/pypi/pycarol>`_. `Documentation for the latest release
<http://pycarol.readthedocs.io/>`__ is hosted on readthedocs.

This will install the minimal dependencies. To install pyCarol with the `dataframes` dependencies use
``pip install pycarol[dataframe]``, or to install with dask+pipeline dependencies use ``pip install pycarol[pipeline,dask]``

The options we have are: `complete`, `dataframe`, `onlineapp`, `dask`, `pipeline`

To install from source:

1. ``pip install -r requirements.txt`` to install the minimal requirements;
2. ``pip install -e . ".[dev]"`` to install the minimal requirements + dev libs;
3. ``pip install -e . ".[pipeline]"`` to install the minimal requirements + pipelines dependencies;
4. ``pip install -e . ".[complete]"`` to install all dependencies;


Initializing pyCarol
--------------------

Carol is the main object to access pyCarol and all Carol's APIs.

.. code:: python

    from pycarol import PwdAuth, Carol
    carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
                  auth=PwdAuth(USERNAME, PASSWORD), organization=ORGANIZATION)


where ``domain`` is the tenant name, ``app_name`` is the Carol's app name, if any, ``auth``
is the authentication method to be used (using user/password in this case) and ``organization`` is the organization
one wants to connect. Carols's URL is build as ``www.ORGANIZATION.carol.ai/TENANT_NAME``

It is also possible to initialize the object with a token generated via user/password. This is useful when creating an
online app that interacts with Carol

.. code:: python

    from pycarol import PwdKeyAuth, Carol
    carol = Carol(domain=TENANT_NAME, app_name=APP_NAME,
                  auth=PwdKeyAuth(pwd_auth_token), organization=ORGANIZATION)


Using API Key
--------------
To use API keys instead of username and password:

.. code:: python

    from pycarol import ApiKeyAuth, Carol

    carol = Carol(domain=DOMAIN,
                  app_name=APP_NAME,
                  auth=ApiKeyAuth(api_key=X_AUTH_KEY),
                  connector_id=CONNECTOR, organization=ORGANIZATION)

In this case one changes the authentication method to ``ApiKeyAuth``. Noticed that one needs to pass the ``connector_id``
too. An API key is always associated to a connector ID. 

It is possible to use pyCarol to generate an API key

.. code:: python

    from pycarol import PwdAuth, ApiKeyAuth, Carol

    carol = Carol(domain=TENANT_NAME, app_name=APP_NAME, organization=ORGANIZATION,
                  auth=PwdAuth(USERNAME, PASSWORD), connector_id=CONNECTOR)
    api_key = carol.issue_api_key()

    print(f"This is a API key {api_key['X-Auth-Key']}")
    print(f"This is the connector Id {api_key['X-Auth-ConnectorId']}")

To get the details of the API key you can do:

.. code:: python

    details = carol.api_key_details(APIKEY, CONNECTORID)


Finally, to revoke an API key:

.. code:: python

    carol.api_key_revoke(CONNECTORID)



Good practice using token
-------------------------

Never write in plain text your password/API token in your application. Use environment variables. pyCarol can use 
environment variables automatically. When none parameter is passed to the Carol constructor pycarol will look for:

 1. ``CAROLTENANT`` for domain
 2. ``CAROLAPPNAME`` for app_name
 3. ``CAROL_DOMAIN`` for environment
 4. ``CAROLORGANIZATION`` for organization
 5. ``CAROLAPPOAUTH`` for auth
 6. ``CAROLCONNECTORID`` for connector_id
 7. ``CAROLUSER`` for carol user email
 8. ``CAROLPWD`` for user password.
 
 e.g., one can create a ``.env`` file like this:

.. code:: python

    CAROLAPPNAME=myApp
    CAROLTENANT=myTenant
    CAROLORGANIZATION=myOrganization
    CAROLAPPOAUTH=myAPIKey
    CAROLCONNECTORID=myConnector

and then

.. code:: python

    from pycarol import Carol
    from dotenv import load_dotenv
    load_dotenv(".env") #this will import these env variables to your execution.
    carol = Carol()


Ingesting data
--------------

From both Staging Tables and Data Models (BQ Layer)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use this method when you need to read only a subset of records and columns or when 
data transformation is needed.

.. code:: python

    from pycarol import BQ, Carol

    bq = BQ(Carol())
    query_str = "SELECT * FROM stg_connectorname_table_name"
    results = bq.query(query_str)


In case one needs a service account with access to BigQuery, the following code can be
used:

.. code:: python

    from pycarol import Carol
    from pycarol.bigquery import TokenManager

    tm = TokenManager(Carol())
    service_account = tm.get_token().service_account


After each execution of ``BQ.query``, the ``BQ`` object will have an attribute called
``job``. This attribute is of type ``bigquery.job.query.QueryJob`` and may be useful for
monitoring/debug jobs.

PyCarol provides access to BigQuery Storage API also. It allows for much faster reading
times, but with limited querying capabilities. For instance, only tables are readable,
so 'ingestion_stg_model_deep_audit' is ok, but 'stg_model_deep_audit' is not (it is a 
view).

.. code:: python

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


From Data Models (RT Layer): Filter queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use this when you need low latency (only if RT layer is enabled).

.. code:: python

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



The result will be ``200`` hits of the query ``json_query``  above, the pagination will be 10, that means in each response
there will be 10 records. The query will return only the fields set in ``FIELDS_ITEMS``.

The parameter ``only_hits = True`` will make sure that only records into the path ``$hits.mdmGoldenFieldAndValues`` will return.
If one wants all the response use ``only_hits = False``. Also, if your filter has an aggregation, one should use
``only_hits = False`` and ``get_aggs=True``, e.g.,


.. code:: python

    from pycarol import Query
    from pycarol.filter import TYPE_FILTER, Filter, CARDINALITY

    json_query = Filter.Builder() \
        .must(TYPE_FILTER(value='datamodelname' + "Golden")) \
        .aggregation(CARDINALITY(name='cardinality', params = ["mdmGoldenFieldAndValues.taxid.raw"], size=40))\
        .build().to_json()

    query = Query(carol, get_aggs=True, only_hits=False)
    query.query(json_query).go()
    query.results


From Data Models (RT Layer): Named queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from pycarol import Query

    named_query = 'revenueHist'  # named query name
    params = {"bin":"1d","cnpj":"24386434000130"}  #query parameters to send.
    results = Query(carol).named(named_query, params=params).go().results

It is possible to use all the parameters used in the filter query, i.e., ``only_hits`` ,
``save_results``, etc. For more information for the possible input parameters check the docstring.

What if one does not remember the parameters for a given named query?


.. code:: python

    named_query = 'revenueHist'  # named query name
    Query(carol).named_query_params(named_query)
    > {'revenueHist': ['*cnpj', 'dateFrom', 'dateTo', '*bin']}  #Parameters starting by * are mandatory.



Sending data
------------

The first step to send data to Carol is to create a connector.

.. code:: python

    from pycarol import Connectors
    connector_id = Connectors(carol).create(name='my_connector', label="connector_label", group_name="GroupName")
    print(f"This is the connector id: {connector_id}")


With the connector Id on hands we can create the staging schema and then create the staging table. Assuming we have
a sample of the data we want to send.

.. code:: python

    from pycarol import Staging

    json_ex = {"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'} }

    staging = Staging(carol)
    staging.create_schema(staging_name='my_stag', data = json_ex,
                          crosswalk_name= 'my_crosswalk' ,crosswalk_list=['name'],
                            connector_name='my_connector')


The json schema will be in the variable ``schema.schema``. The code above will create the following schema:

.. code:: python

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


To send the data  (assuming we have a json with the data we want to send).

.. code:: python

    from pycarol import Staging

    json_ex = [{"name":'Rafael',"email": {"type": "email", "email": 'rafael@totvs.com.br'}   },
               {"name":'Leandro',"email": {"type": "email", "email": 'Leandro@totvs.com.br'}   },
               {"name":'Joao',"email": {"type": "email", "email": 'joao@rolima.com.br'}   },
               {"name":'Marcelo',"email": {"type": "email", "email": 'marcelo@totvs.com.br'}   }]


    staging = Staging(carol)
    staging.send_data(staging_name = 'my_stag', data = json_ex, step_size = 2,
                     connector_id=connectorId, print_stats = True)

The parameter ``step_size`` says how many registers will be sent each time. Remember the the max size per payload is
5MB. The parameter  ``data`` can be a pandas DataFrame.

OBS: It is not possible to create a mapping using pycarol. The Mapping has to be done via the UI



Logging
--------


To log messages to Carol:

.. code:: python

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


These methods will use the current long task id provided by Carol when running your application.
For local environments you need to set that manually first on the beginning of your code:

.. code:: python

    import os
    os.environ['LONGTASKID'] = task_id

We recommend to log only INFO+ information in Carol. If no TASK ID is passed it works as a Console Handler. 

Settings
--------
We can use pyCarol to access the settings of your Carol App.

.. code:: python

    from pycarol.apps import Apps
    app = Apps(carol)
    settings = app.get_settings(app_name='my_app')
    print(settings)


The settings will be returned as a dictionary where the keys are the parameter names and the values are
the value for that parameter. Please note that your app must be created in Carol.


Useful Functions
--------------------

1. ``track_tasks``: Track a list of tasks.

.. code:: python

    from pycarol import Carol
    from pycarol.functions import track_tasks
    carol = Carol()
    def callback(task_list):
      print(task_list)
    track_tasks(carol=carol, task_list=['task_id_1', 'task_id_2'], callback=callback)
  

Release process
----------------
1. Open a PR with your change for `master` branch;
2. Once approved, merge into `master`;
3. In case there are any changes to the default release notes, please update them

