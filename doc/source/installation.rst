Installation
==========================================


PyPi
******************************************

Totvs Labs Pypi Repostory is up and running, and pyCarol 2.12 is published there.
Our repository provides access to public *pypi.pip.org* repository too, as a transparent proxy.

To get access to our repository, you have to configure pip.conf file.

- On Unix the default configuration file is: `$HOME/.config/pip/pip.conf` which respects the `XDG_CONFIG_HOME` environment variable.
- On macOS the configuration file is `$HOME/Library/Application Support/pip/pip.conf` if directory `$HOME/Library/Application Support/pip` exists else `$HOME/.config/pip/pip.conf`.
- On Windows the configuration file is `%APPDATA%\pip\pip.ini`.

Inside a virtualenv:

- On Unix and macOS the file is `$VIRTUAL_ENV/pip.conf`
- On Windows the file is: `%VIRTUAL_ENV%\pip.ini`

There is a shortcut to open this file. Just execute the following command on your terminal:

.. code-block:: none

    pip config edit

*The config*:

Just add the following config in your pip.config

.. code-block:: none

    index = http://nexus3.carol.ai:8080/repository/totvslabspypi/pypi
    index-url = http://nexus3.carol.ai:8080/repository/totvslabspypi/simple
    trusted-host = nexus3.carol.ai

For now, anonymous users have access to this repository.
In the next couple months we gonna add authentication level to this repository.

You can use pip as you used to do.

.. code-block:: none

    pip install pycarol