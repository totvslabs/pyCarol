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

.. _reading-data:

Reading data
------------

.. code-block:: python

    from pycarol import BQ, Carol

    BQ(Carol()).query("SELECT * FROM stg_connectorname_tablename")

.. _carol-in-memory:

Carol In Memory
---------------

.. code-block:: python

    from pycarol import Memory

    memory = Memory()
    memory.add("my_table", [{"id": 1}])
    memory.query("SELECT * FROM my_table")

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
------------------------------------

.. code-block:: python

    carol.call_api("v1/some/endpoint", method="POST")

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
