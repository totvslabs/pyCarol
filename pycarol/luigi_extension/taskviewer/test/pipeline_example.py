from pycarol.luigi_extension import Task, inherit_list


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
from pycarol.luigi_extension.pipetools.pipetools import Pipe

pipeline1 = Pipe([T5(**params)])