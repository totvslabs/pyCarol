from pycarol.pipeline import inherit_list
from pycarol.pipeline import Task
import luigi
from .task_config import Task
import time
# This is important if there are tasks in multiple files.
luigi.auto_namespace(scope=__name__)


class MyTask1(Task):
    param_1 = luigi.IntParameter()

    def easy_run(self, inputs):
        time.sleep(1)
        return self.param_1

@inherit_list(
    MyTask1
)
class MyTask2(Task):
    param_2 = luigi.IntParameter()
    def easy_run(self, inputs):
        my_task_1_output = inputs[0]
        print(f"output MyTask1 is {my_task_1_output}")
        time.sleep(1)
        return self.param_2


def my_function_task(input_task_1, **extra_param):
    print(f"this is the input {input_task_1}")
    print(f"this are the task params {extra_param}")
    my_output = extra_param
    return my_output

@inherit_list(
    MyTask2
)
class MyTask3(Task):
    param_3 = luigi.IntParameter()
    # pycarol will send the inputs of the task as positional and all task parameters as keyword arguments.
    task_function = my_function_task
    

@inherit_list(
    MyTask3
)
class MyTask4(Task):
    param_4 = luigi.IntParameter()
    #look at the notebook to check what is being doing.
    task_notebook = "pipelie_ex/a_notebook_task.ipynb"




