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

#TODO: implement the following methods
class Pipe(object):
    """
    This class should be used to compose a pipeline given a list of tasks. It
    contains many methods to interact with the pipeline as a whole.
    """
    def __init__(self, tasks: list):
        assert isinstance(tasks,list)

        for t in tasks:
            assert isinstance(t,Task)
        self.top_nodes = tasks
        self.dag = get_dag_from_task(tasks)
        self.all_tasks = [k for k in self.dag]

    def remove_all(self):
        """Remove all targets related to this pipeline."""
        for t in self.all_tasks:
            t.remove()

    def remove_upstream(self, task):
        """Remove all targets in this pipeline that depend on the given task."""
        return

    def remove_obsolete(self):
        """Remove all targets whose hash_versions do not match to current version"""
        return
    
    def remove_orphans(self):
        """Remove all targets for which respective downstream targets are not complete"""
        return

    def get_task_by_name(self,name:str)->list:
        """Given the name of a task returns a list of matching task objects"""
        return

    def run(self):
        """Run the whole pipeline"""
        return
    
    def run_partial(self,task):
        """Run the pipeline until given task is achieved"""
        return

    def run_task(self,task):
        """Run single task if requirements are satisfied"""
        return

    # def get_complete(self):
    #     return

    # def describe_targets(self)-> str:
    #     """Returns a text summary informing the status of all related targets."""
    #     return
