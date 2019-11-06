"""
PyCarol Pipeline
=================================

This module defines a framework for building and managing data pipelines. It was
inspired in Luigi's architecture and has it as a backend for the
implementation of the pipeline execution.

In submodule targets, luigi targets classes are implemented. Some of these
targets, make use of Carol Data Storage and are a key feature for running the
pipeline in the cloud.

In submodule tasks, we found Carol extension to luigi Task class. This new
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
version to be used inside Carol jupyter
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