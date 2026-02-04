PyCarol
=======

--------------------

PyCarol is a Python SDK designed to support data ingestion and data access workflows on Carol.
It provides abstractions for authentication, connector and staging management, data ingestion, and querying,
enabling reliable integration with Carol services using Python.
The SDK encapsulates low-level API communication and authentication logic, making data pipelines easier to
build, maintain, and operate.

Table of Contents
-----------------

- `Getting Started <getting-started_>`_
- `Recommended authentication method <recommended-authentication_>`_
- `Explicit authentication methods <explicit-authentication_>`_

  - `Using user/password <auth-user-password_>`_
  - `Using Tokens <auth-tokens_>`_
  - `Using API Key <auth-api-key_>`_

- `Setting up Carol entities <setup-entities_>`_
- `Sending Data <sending-data_>`_
- `Reading data <reading-data_>`_
- `Carol In Memory <carol-in-memory_>`_
- `Logging <logging_>`_

  - `Prerequisites <logging-prerequisites_>`_
  - `Logging messages to Carol <logging-messages_>`_
  - `Notes <logging-notes_>`_

- `Calling Carol APIs <calling-apis_>`_
- `Settings <settings_>`_
- `Useful Functions <useful-functions_>`_
- `Release process <release-process_>`_

.. _getting-started:

Getting Started
---------------

Run ``pip install pycarol`` to install the latest stable version from
`PyPI <https://pypi.org/project/pycarol/>`_.
Documentation for the `latest release <https://pycarol.readthedocs.io/en/2.55.0/>`_
is hosted on `Read the Docs <https://pycarol.readthedocs.io/>`_.

This will install the minimal dependencies. To install pyCarol with the
``dataframe`` dependencies::

    pip install pycarol[dataframe]

To install with dask and pipeline dependencies::

    pip install pycarol[pipeline,dask]

Available options: ``complete``, ``dataframe``, ``onlineapp``, ``dask``, ``pipeline``.

To install from source:

1. ``pip install -r requirements.txt``
2. ``pip install -e . ".[dev]"``
3. ``pip install -e . ".[pipeline]"``
4. ``pip install -e . ".[complete]"``

.. _recommended-authentication:

Recommended authentication method
---------------------------------

Never write passwords or API tokens in plain text. Use environment variables.
When no parameters are passed to the ``Carol`` constructor, pyCarol automatically
reads the following variables:

1. ``CAROLTENANT`` – tenant name
2. ``CAROLAPPNAME`` – app name
3. ``CAROL_DOMAIN`` – environment
4. ``CAROLORGANIZATION`` – organization
5. ``CAROLAPPOAUTH`` – authentication token
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

Loading environment variables:

.. code-block:: python

    from pycarol import Carol
    from dotenv import load_dotenv

    load_dotenv(".env")
    carol = Carol()

.. _explicit-authentication:

Explicit authentication methods
-------------------------------

Carol is the main object to access pyCarol and Carol APIs.

.. _auth-user-password:

Using user/password
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pycarol import PwdAuth, Carol

    carol = Carol(
        domain=TENANT_NAME,
        app_name=APP_NAME,
        auth=PwdAuth(USERNAME, PASSWORD),
        organization=ORGANIZATION
    )

.. _auth-tokens:

Using Tokens
~~~~~~~~~~~~

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

.. _auth-api-key:

Using API Key
~~~~~~~~~~~~~

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

.. _setup-entities:

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

Creating staging schema:

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

.. _sending-data:

Sending Data
------------

.. code-block:: python

    from pycarol import Staging

    staging = Staging(carol)
    staging.send_data(
        staging_name='my_stag',
        data=[{"name": "Rafael"}],
        connector_id=CONNECTORID,
        print_stats=True
    )

.. _reading-data:

Reading data
------------

.. code-block:: python

    from pycarol import BQ, Carol

    bq = BQ(Carol())
    results = bq.query("SELECT * FROM stg_connectorname_tablename")

.. _carol-in-memory:

Carol In Memory
---------------

.. code-block:: python

    from pycarol import Carol, Memory, BQStorage

    storage = BQStorage(Carol())
    memory = Memory()

    data = storage.query("ingestion_stg_connectorname_tablename")
    memory.add("my_table", data)
    table = memory.query("SELECT * FROM my_table")

.. _logging:

Logging
-------

.. _logging-prerequisites:

Prerequisites
~~~~~~~~~~~~~

.. code-block:: python

    import os
    os.environ['LONGTASKID'] = task_id

.. _logging-messages:

Logging messages to Carol
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pycarol import Carol, CarolHandler
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(CarolHandler(Carol()))

    logger.info("This is an info message")

.. _logging-notes:

Notes
~~~~~

- Logs are associated with the current long task when running inside Carol
- Without task ID, logs fall back to console
- Prefer ``INFO`` level and above

.. _calling-apis:

Calling Carol APIs
------------------

.. code-block:: python

    carol.call_api(
        'v1/tenantApps/subscribe/carolApps/{carol_app_id}',
        method='POST'
    )

.. _settings:

Settings
--------

.. code-block:: python

    from pycarol.apps import Apps

    app = Apps(carol)
    settings = app.get_settings(app_name='my_app')

.. _useful-functions:

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

.. _release-process:

Release process
---------------

1. Open a PR targeting ``main``
2. Merge after approval
3. Update release notes if needed
4. Update README when features change

--------------------

Made with ❤ at TOTVS IDeIA
