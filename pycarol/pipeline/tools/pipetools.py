from pycarol.pipeline import Task
from pycarol.pipeline.utils import build_dag

def luigi_get_sons(task) -> list:
    """
    Returns a list of required tasks. This is used in build_dag
    Args:
        task: luigi Task

    Returns:
        l: list of luigi Task

    """
    return task.requires()


def get_dag_from_task(task:list) -> dict:
    """
    Wrapper around generic build_dag.
    Args:
        task: list of proper luigi tasks

    Returns:
        dag: dict encoding a DAG

    """
    dag = build_dag(task,luigi_get_sons)
    return dag


class Pipe(object):
    def __init__(self, tasks: list):
        assert isinstance(tasks,list)

        for t in tasks:
            assert isinstance(t,Task)
        self.top_nodes = tasks
        self.dag = get_dag_from_task(tasks)
        self.all_tasks = [k for k in self.dag]
    def remove_upstream(self, task):
        return
    def remove_all(self):
        return
    def describe_targets(self):
        return
    def get_task_by_name(self,name):
        return
    def get_dag(self):
        return self.dag