Appendix
========

1. Setting up a local dev environment
-------------------------------------

When Carol runs a process, it injects into the Docker container some
environment variables that will be read when we call:

::

    login = Carol()

When running our app locally we have to inject these variables
ourselves.

For that, we create an .env file with the following content:

::

    # datascience.carol.ai/mltutorial
    CAROLAPPNAME=bhponlineapp
    CAROLAPPOAUTH=<put your token here>
    CAROLCONNECTORID=d69c6f0ea6334838a75a38b543c0214b
    CAROLORGANIZATION=datascience
    CAROLTENANT=mltutorial
    ALGORITHM_NAME=bhponlineapp

CAROLAPPNAME: the name of the app created in Carol. To see how you can
create an app in Carol please refer to the previous chapters on this manual.

CAROLAPPOAUTH: the api key (access token) created in Carol. To see how
you can create an api key please refer to `Generating an access
token <https://tdn.totvs.com/pages/releaseview.action?pageId=552107176#id-2.Autentica%C3%A7%C3%A3o-ConnectorToken(APIKey)>`__.

CAROLCONNECTORID: the connector id attached to the api key added in
CAROLAPPOAUTH.

CAROLORGANIZATION: the name of the organization in which your app has
been created.

CAROLTENANT: the name of the environment in which your app has been
created.

Running our app locally
-----------------------

Carol runs its apps inside Docker containers for that we need to have
built images created from our app code.

When we want to run our app locally and simulate what Carol does in the
real scenario we use the following docker commands:

Building the docker image:
~~~~~~~~~~~~~~~~~~~~~~~~~~

It creates a docker image using the recipe we have created in our
Dockerfile.

We build an image by running:

::

    docker build -t <docker name> .

In ``<docker name>`` you can add any name that relates to your app.

In the real scenario this process happens when we Build a Carol app.

Building the docker image:
~~~~~~~~~~~~~~~~~~~~~~~~~~

It runs the docker image created in the previous step.

We run an image by running:

::

    docker run --rm -it -p 5000:5000 --env-file .env <docker name>

In ``<docker name>`` you need to call the same name defined in the build
process.

Here we use the ``.env`` file that simulates the injection of environment
variables.

In the real scenario this process happens when we Run a process in a
Carol app.
