PyCarol
=======

.. raw:: html

   <hr/>

PyCarol is a Python SDK designed to support data ingestion and data access workflows on Carol.
It provides abstractions for authentication, connector and staging management, data ingestion, and querying, enabling reliable integration with Carol services using Python.
The SDK encapsulates low-level API communication and authentication logic, making data pipelines easier to build, maintain, and operate.

Table of Contents
-----------------

- Getting Started_
- Recommended authentication method_
- Explicit authentication methods_

  - `1. Using user/password`_
  - `2. Using Tokens`_
  - `3. Using API Key`_

- Setting up Carol entities_
- Sending Data_
- Reading data_
- Carol In Memory_
- Logging_

  - Prerequisites_
  - Logging messages to Carol_
  - Notes_

- Calling Carol APIs_
- Settings_
- Useful Functions_
- Release process_

Getting Started
---------------

Run ``pip install pycarol`` to install the latest stable version from `PyPI <https://pypi.org/project/pycarol/>`_.
Documentation for the `latest release <https://pycarol.readthedocs.io/en/2.55.0/>`_ is hosted on `Read the Docs <https://pycarol.readthedocs.io/>`_.

This will install the minimal dependencies. To install pyCarol with the ``dataframes`` dependencies use::

    pip install pycarol[dataframe]

Or to install with dask+pipeline dependencies::

    pip install pycarol[pipeline,dask]

The available options are: ``complete``, ``dataframe``, ``onlineapp``, ``dask``, ``pipeline``.

To install from source:

1. ``pip install -r requirements.txt``
2. ``pip install -e . ".[dev]"``
3. ``pip install -e . ".[pipeline]"``
4. ``pip install -e . ".[complete]"``

Recommended authentication method
---------------------------------

Never write in plain text your password or API token in your application. Use environment variables.
pyCarol automatically reads environment variables when no parameters are passed to the ``Carol`` constructor.

Variables used:

1. ``CAROLTENANT`` – domain or tenant name
2. ``CAROLAPPNAME`` – app name
3. ``CAROL_DOMAIN`` – environment or tenant name
4. ``CAROLORGANIZATION`` – organization
5. ``CAROLAPPOAUTH`` – auth
6. ``CAROLCONNECTORID`` – connector ID
7. ``CAROLUSER`` – user email
8. ``CAROLPWD`` – user password

Carol URL format::

    www.ORGANIZATION.carol.ai/TENANT_NAME

Example ``.env`` file::

    CAROLAPPNAME=myApp
    CAROLTENANT=myTenant
    CAROLORGANIZATION=myOrganization
    CAROLAPPOAUTH=myAPIKey
    CAROLCONNECTORID=myConnector

Loading the environment variables:

.. code-block:: python

    from pycarol import Carol
    from dotenv import load_dotenv

    load_dotenv(".env")
    carol = Carol()

Explicit authentication methods
-------------------------------

Carol is the main object to access pyCarol and all Carol APIs.

1. Using user/password
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pycarol import PwdAuth, Carol

    carol = Carol(
        domain=TENANT_NAME,
        app_name=APP_NAME,
        auth=PwdAuth(USERNAME, PASSWORD),
        organization=ORGANIZATION
    )

2. Using Tokens
~~~~~~~~~~~~~~

.. code-block:: python

    from pycarol import PwdKeyAuth, Carol

    carol = Carol(
        domain=TENANT_NAME,
        app_name=APP_NAME,
        auth=PwdKeyAuth(pwd_auth_token),
        organization=ORGANIZATION
    )

For Fluig Identity:

.. code-block:: python

    from pycarol import PwdFluig, Carol

    carol = Carol(
        domain=TENANT_NAME,
        auth=PwdFluig(USER_LOGIN),
        organization=ORGANIZATION
    )

3. Using API Key
~~~~~~~~~~~~~~~~

Generating an API key:

.. code-block:: python

    from pycarol import PwdAuth, ApiKeyAuth, Carol

    carol = Carol(
        domain=TENANT_NAME,
        app_name=APP_NAME,
        organization=ORGANIZATION,
        auth=PwdAuth(USERNAME, PASSWORD),
        connector_id=CONNECTORID
    )

    api_key = carol.issue_api_key()

Authenticating with API key:

.. code-block:: python

    from pycarol import ApiKeyAuth, Carol

    carol = Carol(
        domain=DOMAIN,
        app_name=APP_NAME,
        auth=ApiKeyAuth(api_key=X_AUTH_KEY),
        connector_id=CONNECTORID,
        organization=ORGANIZATION
    )

Setting up Carol entities
------------------------

Creating a connector:

.. code-block:: python

    from pycarol import Connectors

    connector_id = Connectors(carol).create(
        name='my_connector',
        label='connector_label',
        group_name='GroupName'
    )

Creating staging schema and table:

.. code-block:: python

    from pycarol import Staging

    json_ex = {
        "name": "Rafael",
        "email": {
            "type": "email",
            "email": "rafael@totvs.com.br"
        }
    }

    staging = Staging(carol)
    staging.create_schema(
        staging_name='my_stag',
        data=json_ex,
        crosswalk_name='my_crosswalk',
        crosswalk_list=['name'],
        connector_name='my_connector'
    )

Sending Data
------------

.. code-block:: python

    from pycarol import Staging

    json_ex = [
        {"name": "Rafael", "email": {"type": "email", "email": "rafael@totvs.com.br"}},
        {"name": "Leandro", "email": {"type": "email", "email": "leandro@totvs.com.br"}},
        {"name": "Joao", "email": {"type": "email", "email": "joao@rolima.com.br"}},
        {"name": "Marcelo", "email": {"type": "email", "email": "marcelo@totvs.com.br"}},
    ]

    staging = Staging(carol)
    staging.send_data(
        staging_name='my_stag',
        data=json_ex,
        step_size=2,
        connector_id=CONNECTORID,
        print_stats=True
    )

Reading data
------------

Querying BigQuery:

.. code-block:: python

    from pycarol import BQ, Carol

    bq = BQ(Carol())
    results = bq.query("SELECT * FROM stg_connectorname_tablename")

Carol In Memory
---------------

.. code-block:: python

    from pycarol import Carol, Memory, BQStorage
    from dotenv import load_dotenv

    load_dotenv()
    carol = Carol()

    storage = BQStorage(carol)
    memory = Memory()

    data = storage.query(
        'ingestion_stg_connectorname_tablename',
        column_names=['tenantid', 'processing', '_ingestionDatetime']
    )

    memory.add("my_table", data)
    table = memory.query("SELECT * FROM my_table")

Logging
-------

Prerequisites
~~~~~~~~~~~~~

.. code-block:: python

    import os
    os.environ['LONGTASKID'] = task_id

Logging messages to Carol
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pycarol import Carol, CarolHandler
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    handler = CarolHandler(Carol())
    handler.setLevel(logging.INFO)

    logger.addHandler(handler)
    logger.info("This is an info message")

Calling Carol APIs
------------------

.. code-block:: python

    carol.call_api(
        'v1/tenantApps/subscribe/carolApps/{carol_app_id}',
        method='POST'
    )

Settings
--------

.. code-block:: python

    from pycarol.apps import Apps

    app = Apps(carol)
    settings = app.get_settings(app_name='my_app')

Useful Functions
----------------

Tracking tasks:

.. code-block:: python

    from pycarol.functions import track_tasks

    def callback(task_list):
        print(task_list)

    track_tasks(
        carol=carol,
        task_list=['task_id_1', 'task_id_2'],
        callback=callback
    )

Release process
---------------

1. Open a PR targeting the ``main`` branch
2. Merge after approval
3. Update release notes if needed
4. Update README when features change

.. raw:: html

   <hr/>

Made with ❤ at TOTVS IDeIA
