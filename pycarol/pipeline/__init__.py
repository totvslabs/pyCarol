"""
PyCarol Pipeline
=================================

This module defines a framework for building and managing data pipelines. It was inspired in Luigi's architecture and
has it as a backend for the implementation of the pipeline execution.

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
from pycarol.pipeline.targets.deprecated_targets import (
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

from pycarol.pipeline.viewer.task_visualization import (
    Visualization
)
