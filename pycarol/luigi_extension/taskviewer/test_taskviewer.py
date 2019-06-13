from pytest import mark
from pycarol.luigi_extension.task import Task,inherit_list
from .taskviewer import *

class T1(Task):
    pass

@inherit_list(T1)
class T2(Task):
    pass

@inherit_list(T1,T2)
class T3(Task):
    pass

@inherit_list(T2,T3)
class T4(Task):
    def easy_run(self, inputs):
        raise Exception

@inherit_list(T1,T3)
class T5(Task):
    pass

params = {}


def test_task_run():
    T4(**params).buildme()

def test_get_dag_from_task():
    d = get_dag_from_task([T5(**params)],luigi_get_sons)
    print(d)
