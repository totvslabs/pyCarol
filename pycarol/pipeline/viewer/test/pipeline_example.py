from pycarol.pipeline import Task, inherit_list
#TODO: update tasks definitions to new format
#TODO: use same pipeline to test pipetools

class T1(Task):
    pass


@inherit_list(T1)
class T2(Task):
    pass


@inherit_list(T1, T2)
class T3(Task):
    pass


@inherit_list(T2, T3)
class T4(Task):
    def easy_run(self, inputs):
        raise Exception


@inherit_list(T1, T3)
class T5(Task):
    pass


params = {}
from pycarol.pipeline.tools import Pipe

pipeline1 = Pipe([T5(**params)])