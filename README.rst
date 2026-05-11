PyCarol
=======

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

  - `Staging batch API: Batch ingestion <staging-batch-api_>`_
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
Documentation is hosted on `Read the Docs <https://pycarol.readthedocs.io/>`_.

.. _recommended-authentication:

Recommended authentication method
---------------------------------

Never write passwords or API tokens in plain text.
Use environment variables whenever possible.

Carol URL format::

    www.ORGANIZATION.carol.ai/TENANT_NAME

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

.. _auth-api-key:

Using API Key
~~~~~~~~~~~~~

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
-------------------------

.. code-block:: python

    from pycarol import Connectors

    connector_id = Connectors(carol).create(
        name="my_connector",
        label="connector_label"
    )

.. _sending-data:

Sending Data
------------

.. code-block:: python

    from pycarol import Staging

    Staging(carol).send_data(
        staging_name="my_stag",
        data=[{"name": "Rafael"}],
        connector_id=CONNECTORID
    )

.. _staging-batch-api:

Staging batch API: Batch ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To group multiple ``send_data()`` calls under one batch (e.g. for Carol to process as a unit), use
``start_batch()`` and ``end_batch()``. Each request is tagged with ``batchId`` and ``batchIdSequence``.
If you do not start a batch explicitly, a batch is auto-started and auto-ended around a single
``send_data()`` call.

- **``Staging.start_batch()``**: Starts a batch, generates a ``batchId``, returns it.
- **``Staging.end_batch()``**: Sends the batch summary to Carol and clears the current batch.
- **``Staging.send_data()``**: When a batch is active, appends ``batchId`` and ``batchIdSequence`` to the intake URL.

.. code-block:: python

    from pycarol import Carol, Staging
    from dotenv import load_dotenv
    load_dotenv()

    carol = Carol()
    json_ex = [
        {"name": "Rafael", "email": {"type": "email", "email": "rafael@totvs.com.br"}},
        {"name": "Leandro", "email": {"type": "email", "email": "Leandro@totvs.com.br"}},
    ]
    staging = Staging(carol)

    # Single send_data: batch is generated internally
    staging.send_data(staging_name="test_batch", data=json_ex, step_size=1,
                      connector_id=CONNECTORID, print_stats=True)

    # User-managed batch for multiple intake calls
    staging.start_batch()
    staging.send_data(staging_name="test_batch", data=json_ex, step_size=1,
                      connector_id=CONNECTORID, print_stats=True)
    staging.send_data(staging_name="test_batch", data=json_ex, step_size=4,
                      connector_id=CONNECTORID, print_stats=True)
    staging.end_batch()

.. _reading-data:

Reading data
------------

.. code-block:: python

    from pycarol import BQ, Carol

    BQ(Carol()).query("SELECT * FROM stg_connectorname_tablename")

.. _carol-in-memory:

Carol In Memory
---------------

PyCarol provides an easy way to work with in-memory data using the Memory class, built on top of DuckDB.
Queries are executed locally over in-memory data, without triggering BigQuery jobs or consuming BigQuery
slots, and results are returned as pandas DataFrames. The recommended usage is with ``BQStorage`` objects.

On ``BQStorage`` you can optionally indicate the dataset by declaring ``dataset_id``. 
If you don't, it will default to Carol's dataset.

.. code-block:: python

    from pycarol import Carol, Memory, BQStorage
    from dotenv import load_dotenv

    load_dotenv()
    carol = Carol()

    storage = BQStorage(carol)
    memory = Memory()

    t = storage.query(
        "ingestion_stg_connectorname_tablename",
        column_names=["tenantid", "processing", "_ingestionDatetime"],
        max_stream_count=50
    )
    memory.add("my_table", t)

    table = memory.query("SELECT * FROM my_table")
    print(table)

The syntax of Carol In Memory follows DuckDB `SQL Syntax <https://duckdb.org/docs/stable/sql/introduction>`_.

.. _logging:

Logging
-------

.. _logging-prerequisites:

Prerequisites
~~~~~~~~~~~~~

Set ``LONGTASKID`` when running locally.

.. _logging-messages:

Logging messages to Carol
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import logging
    from pycarol import CarolHandler, Carol

    logger = logging.getLogger(__name__)
    logger.addHandler(CarolHandler(Carol()))
    logger.info("Hello Carol")

.. _logging-notes:

Notes
~~~~~~~~~~

- Logs are linked to long tasks
- Console fallback when task ID is missing

.. _calling-apis:

Calling Carol APIs
------------------

In addition to the high-level abstractions provided by pyCarol, it is also possible to call Carol APIs directly when needed.
This is useful for endpoints that are not yet covered by specific SDK methods.

.. code-block:: python

    carol.call_api(
        "v1/tenantApps/subscribe/carolApps/{carol_app_id}",
        method="POST"
    )

.. _settings:

Settings
----------------

.. code-block:: python

    from pycarol.apps import Apps
    Apps(carol).get_settings(app_name="my_app")

.. _useful-functions:

Useful Functions
--------------------------------

.. code-block:: python

    from pycarol.functions import track_tasks
    track_tasks(carol, ["task1", "task2"])

.. _release-process:

Release process
------------------------------

1. Open PR to ``main``
2. Merge after approval
3. Update README if needed

Made with ❤ at TOTVS IDeIA
