# Carol environment: how to get started

If you need to know more about the process of creating Apps on Carol please refer to []().

# Online app

An online app is a server that runs on top of Carol. In this tutorial we will be covering the creation of an online app with a user interface.

Our online app is created using [streamlit](https://streamlit.io/), which is framework for easily create web apps based on data.

On this example we present a simple online app UI that loads the Boston House Prices model trained in the [batch app tutorial]().

Our UI allow us to add all information from a house and it presents a price for that house predicted by our model.

# Online app UI structure

An online app UI usually have the follow structure:

```
app.py
Dockerfile
manifest.json
requirements.txt
```

The `app.py` will be the Docker image entrypoint, it is where we create the web app using streamlit.

The necessary packages to our api are saved in a `requirements.txt` file and they will be installed when the Dockerfile is run on the docker build process.

`Dockerfile`:
Our Dockerfile simply copy the files from our repository to a container and install the packages that we have added to `requirements.txt`. Finally, we expose the port 5000 which is the default port for online apps in Carol and we start the streamlit server.

```
FROM python:3.8

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

ADD . /app

EXPOSE 5000

CMD streamlit run --server.port 5000 app.py
```


This forementioned structure covers much of the cases that you may need. Therefore, we usually only add our logic to `app.py`, and packages to `requirements.txt`. Keeping the `Dockerfile` as it is.

## Setting up a dev environment

When Carol runs a process, it injects into the Docker container some environment variables that will be read when we call:
```
login = Carol()
```

When running our app locally we have to inject these variables ourselves.

For that, we create an .env file with the following content:

```
# datascience.carol.ai/mltutorial
CAROLAPPNAME=bhponlineapp
CAROLAPPOAUTH=<put your token here>
CAROLCONNECTORID=d69c6f0ea6334838a75a38b543c0214b
CAROLORGANIZATION=datascience
CAROLTENANT=mltutorial
ALGORITHM_NAME=bhponlineapp
```

CAROLAPPNAME: the name of the app created in Carol. To see how you can create an app in Carol please refer to []()
CAROLAPPOAUTH: the api key (access token) created in Carol. To see how you can create an api key please refer to [Generating an access token]()
CAROLCONNECTORID: the connector id attached to the api key added in CAROLAPPOAUTH.
CAROLORGANIZATION: the name of the organization in which your app has been created.
CAROLTENANT: the name of the environment in which your app has been created.


## Running our app locally

Carol runs its apps inside Docker containers for that we need to have built images created from our app code.

When we want to run our app locally and simulate what Carol does in the real scenario we use the following docker commands:

### Building the docker image:

It creates a docker image using the recipe we have created in our Dockerfile.

We build an image by running:

```
docker build -t <docker name> .
```

In `<docker name>` you can add any name that relates to your app.

In the real scenario this process happens when we Build a Carol app.

### Building the docker image:

It runs the docker image created in the previous step.

We run an image by running:

```
docker run --rm -it -p 5000:5000 --env-file .env <docker name>
```

In `<docker name>` you need to call the same name defined in the build process.

Here we use the `.env` file that simulates the injection of enviroment variables.

In the real scenario this process happens when we Run a process in a Carol app.

## Deploying our app in Carol