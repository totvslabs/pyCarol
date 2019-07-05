"""
PyCarol Pipeline
=================================

This module defines a framework for building and managing data pipelines. It was inspired in Luigi's architecture and
has it as a backend for the implementation of the pipeline execution.

"""
#TODO(renan): "from pycarol.luigi_extension.targets import *" is not working
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