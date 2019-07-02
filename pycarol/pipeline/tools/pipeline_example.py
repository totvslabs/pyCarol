from pycarol.pipeline.task import Task, inherit_list

from functools import partial
import numpy as np

f1 = partial(np.random.rand,50)
f2 = partial(np.random.rand,1,50)
def f3(x,y,**params):
    return x*y

class T1(Task):
    task_function = f1


class T2(Task):
    task_function = f2


@inherit_list(T1,T2)
class T3(Task):
    task_function = f3


