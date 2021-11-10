# Converting the app to scripting

It the previous sections you learned how to deploy you code into a Carol App. We have created a batch app with one process to train and one process to predict. Both processes were Jupyter notebooks. In this section we are going to show how to deploy the same app with python scripts.

## App structure
The structure of this app is as follows:
```
app/
    predict.py
    run.py
    train.py
Dockerfile
manifest.json
requirements.txt
```

Now instead of having two Docker images, one for each process, we are going to have one docker for two processes. The run.py will be the Docker image entrypoint, and based on the process name it will run predict.py or train.py:

```
import os

from dotenv import load_dotenv

from .app import train, predict

PROCESSES = {
    "train": train.run,
    "predict": predict.run,
}


if __name__ == "__main__":
    load_dotenv()
    ALGORITHM_NAME = os.environ.get("ALGORITHM_NAME")
    PROCESS_NAMES = list(PROCESSES.keys())
    if ALGORITHM_NAME is None:
        raise ValueError("ALGORITHM_NAME environment var does not exist.")
    if ALGORITHM_NAME not in PROCESS_NAMES:
        raise ValueError(
            f"ALGORITHM_NAME env var must be one of the following: {PROCESS_NAMES}"
        )

    PROCESSES[ALGORITHM_NAME]()
```

The ALGORITHM_NAME environment var is injected by Carol into the Docker image before running the process. That name come directly from the manifest file:

```
{
    "batch": {
        "processes": [
            {
                "name": "bhptrainmodelnb",
                "algorithmName": "train",
            },
```

## Setting up a dev environment

Notice that there is the use of the dotenv module. It will help us to have a dev environment. When Carol runs a process, it injects into the Docker image some environment variables that will be read when we call:
```
login = Carol()
```
In Carol __init__ method, it reads some variables like the connector id, tenant id, organization id, and authentication token.

When running your apps locally, you can emulate this process by injecting those variables on your environment, so that a call to Carol() will give you an object with access to Carol's endpoints.

For that purpose, create a file called .env:
```
# datascience.carol.ai/mltutorial
CAROLAPPNAME=bhptrainmodelnb
CAROLAPPOAUTH=<put your token here>
CAROLCONNECTORID=d69c6f0ea6334838a75a38b543c0214b
CAROLORGANIZATION=datascience
CAROLTENANT=mltutorial
ALGORITHM_NAME=bhptrainmodelnb
```

By calling load_dotenv(), it loads the .env file and create those variables in the environment. So in you dev environment you can place a .env file (do not share it) with those environments, and the code that you use for dev can be directly reused in the production code.

## The Dockerfile

Change the dockerfile accordingly, to have a single entrypoint:
```
FROM totvslabs/pycarol:2.40.0

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

ADD . /app

CMD ["python", "/app/post_audit/run.py"]
```


The manifest file, as well as the full code is available at github.com/totvslabs/pyCarol/tutorial/code/checkpoint3.


[Go back to main page](../../)
