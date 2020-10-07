"""
PyCarol Pipeline
================

This module defines a framework for building and managing data pipelines. It was
inspired in Luigi's architecture and has it as a backend for the
implementation of the pipeline execution.

In submodule targets, luigi targets classes are implemented. Some of these
targets, make use of Carol Data Storage and are a key feature for running the
pipeline in the cloud.

In submodule tasks, we found Carol's extension to luigi.Task class. This new
class, combined with some decorators, allows a more efficient description of
the pipeline. It is also in this submodule, that we implement the notebook
task feature, which allows to write task definitions directly in jupyter
notebooks.

In submodule tools, we have tools to interact with the pipeline as a whole.
This inserts an abstraction layer over luigi and provides some features that
are not supported in luigi.

An app pipeline is mainly described using classes from submodules tasks and
targets. Finally, the top pipeline tasks together with the pipeline
parameters definitions are used to instantiate an object of class Pipe,
from submodule tools. This object should be used to make all interactions
with the pipeline, like running, removing targets and related actions.

Finally, submodule viewer implements a dash web app to visualize an object Pipe
and, thus, visualize a given pipeline. This web app has also a jupyter
version to be used inside jupyter lab.

Example
-------

This is a very simplified case when we will create a task that will fetch data from carol in one task and process in
a second task.

.. code:: python

    from dotenv import load_dotenv
    load_dotenv(override=True) #load env variable to create the Carol instance
    from pycarol import Carol, Staging
    from pycarol.pipeline import inherit_list, Task
    import luigi

    @inherit_list(
    )
    class Ingestion(Task):

        connector_name = luigi.Parameter()
        staging_name = luigi.Parameter()

        def easy_run(self, inputs):
            staging_name = self.staging_name
            connector_name = self.connector_name
            stag  = Staging(Carol())
            cds=True
            df = stag.fetch_parquet(staging_name=staging_name, connector_name=connector_name,cds=cds )
            return df


    @inherit_list(
        Ingestion,
    )
    class DataProcess(Task):

        connector_name = luigi.Parameter()
        staging_name = luigi.Parameter()

        def easy_run(self, inputs):
            df = inputs[0] #since there is only one requirement.
            ... #any processing
            return df

    task = [DataProcess(connector_name='new_connector', staging_name='iris')]
    luigi.build(task, local_scheduler=True)

Notice that we can start the pipeline with luigi.build. Pycarol has a method for that too.
If  a single task needs to be build, one can call, from the example above,

.. code:: python

    task = DataProcess(connector_name='new_connector', staging_name='iris')
    task.buildme(local_scheduler=True,)

It will have exactly the same behavior.

"""

from .task import (
    Task,
    WrapperTask,
    inherit_list,
    inherit_dict
)

from .targets import (
    CDSTarget,
    PickleTarget,
    KerasTarget,
    DummyTarget,
    JsonTarget,
    FeatherTarget,
    PytorchTarget,
    ParquetTarget,
    FileTarget,
    LocalTarget
)
from .targets.deprecated_targets import (
    PicklePyCarolTarget,
    PytorchPyCarolTarget,
    KerasPyCarolTarget,
    PickleLocalTarget,
    KerasLocalTarget,
    PytorchLocalTarget,
    JsonLocalTarget,
    FeatherLocalTarget,
    ParquetPyCarolTarget
)

from .tools import (
    Pipe,
)