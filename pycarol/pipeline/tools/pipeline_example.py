from pycarol.pipeline.task import Task, inherit_list
from pycarol.pipeline.tools import Pipe

from functools import partial
import numpy as np
#TODO: improve pipeline_example
def f1():
    return np.ones([50,1])

def f2():
    return np.ones([1,40])

def f3(x,y,**params):
    return x*y

class T1(Task):
    task_function = f1


class T2(Task):
    task_function = f2


@inherit_list(T1,T2)
class T3(Task):
    task_function = f3

params = {}

pipeline1 = Pipe([T3],params)