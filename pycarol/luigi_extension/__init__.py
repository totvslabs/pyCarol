from .task import(
    Task,
    inherit_list,
    inherit_dict
)

from .targets import (
    LocalTarget,
    PickleLocalTarget,
    KerasLocalTarget,
    DummyTarget,
    JsonLocalTarget,
    FeatherLocalTarget,
    PytorchLocalTarget,
    PicklePyCarolTarget,
    PytorchPyCarolTarget,
    KerasPyCarolTarget,
)

from .taskviewer import (
    TaskViewer,
)

# from .dockertask import (
#     EasyDockerTask
# )

# from .kubernetestask import KubernetesJobTask