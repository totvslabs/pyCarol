from pycarol.pipeline import Task
from pycarol.pipeline.utils import build_dag
from pycarol.pipeline.utils import breadth_first_search, get_reverse_dag, find_root_in_dag


import luigi

def luigi_get_sons(task) -> list:
    """
    Returns a list of required tasks. This is used in build_dag
    Args:
        task: luigi Task

    Returns:
        l: list of luigi Task

    """
    return task.requires_list


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
    def __init__(self, tasks: list, params: dict):
        assert isinstance(tasks,list)

        for t in tasks:
            assert issubclass(t,Task)
        self.top_nodes = tasks # top nodes are root nodes
        self.dag = get_dag_from_task(tasks)
        self.rev_dag = get_reverse_dag(self.dag)
        self.leaf_nodes = find_root_in_dag(self.rev_dag) #  leaf nodes are root nodes of rev dag
        self.all_tasks = [k for k in self.dag]
        assert isinstance(params,dict)
        self.params = params

    def remove_all(self):
        """Remove all targets related to this pipeline."""
        for t in self.all_tasks:
            try:
                t(**self.params).remove()
            except FileNotFoundError:
                pass

    def remove_upstream(self, tasks:list):
        """Remove all targets in this pipeline that depend on the given tasks."""
        assert isinstance(tasks,list)
        traverse_dag_generator = breadth_first_search( self.rev_dag,tasks)
        for task_list in traverse_dag_generator:
            for t in task_list:
                t.remove()
    
    def remove_orphans(self):
        """Remove all targets for which respective downstream targets are not complete"""
        def downstream_complete(dag,top_nodes,downstream_complete_dict):
            for task in top_nodes:
                if task in downstream_complete_dict:
                    continue
                sons = dag[task]
                if sons: # recursion step
                    downstream_complete_dict[task] = task.complete() and \
                        all([
                            downstream_complete(dag,[t],downstream_complete_dict) 
                            for t in sons
                            ])
                else: # stop recursion step
                    downstream_complete_dict[task] = task.complete()
        downstream_complete_dict = {}
        downstream_complete(self.dag,self.top_nodes,downstream_complete_dict)

        for task, is_downstream_complete in downstream_complete_dict.items():
            if  not is_downstream_complete:
                task.remove()
        return

    def remove_obsolete(self):
        """Remove all targets whose hash_versions do not match to current version"""
        return

    def run(self):
        """Run the whole pipeline"""
        tasks = [t(**self.params) for t in self.top_nodes]
        luigi.build(tasks,local_scheduler = True)
    
