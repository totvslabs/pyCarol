from pycarol.pipeline import Pipe
import luigi
from pipelie_ex import some_tasks, task_config
params = task_config.params


#note that in pipelines we send only the task class not the instance. 
pipe = Pipe([some_tasks.MyTask4], params)
