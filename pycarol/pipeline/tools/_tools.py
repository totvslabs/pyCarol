import luigi
import copy
from pycarol.pipeline import Task
from pycarol.pipeline.utils import (
    build_dag,
    breadth_first_search,
    get_reverse_dag,
    find_root_in_dag,
)

from luigi.task import flatten
from collections import defaultdict


class Pipe(object):
    """
    This class should be used to compose a pipeline given a list of tasks. It
    contains many methods to interact with the pipeline as a whole.
    """
    def __init__(self, tasks: list, params:dict):
        assert isinstance(tasks,list)
        assert params is None or isinstance(params,dict)
        assert _tasks_are_class(tasks)
        if _tasks_are_instance(tasks):
            raise NotImplementedError(f"Currently, we cannot create Pipe objects from instantiated tasks.")

        self.params = copy.deepcopy(params)
        self.top_nodes = tasks # top nodes are root nodes
        #self.dag = _get_dag_from_task(tasks)
        self.top_nodes = [t(**self.params) for t in self.top_nodes]
        self.dag = _get_dag_(defaultdict(set), self.top_nodes)
        self.dag = {i: list(j) for i,j in self.dag.items()}
        #self.dag = _get_instances_from_classes(self.dag, self.params)
        self.rev_dag = get_reverse_dag(self.dag)
        self.leaf_nodes = find_root_in_dag(self.rev_dag) #  leaf nodes are root nodes of rev dag
        self.all_tasks = [k for k in self.dag]
        

    def remove_all(self):
        """Remove all targets related to this pipeline."""
        for t in self.all_tasks:
            try:
                t.remove()
            except FileNotFoundError:
                pass

    def remove_upstream(self, tasks:list):
        """Remove all targets in this pipeline that depend on the given tasks."""
        assert isinstance(tasks,list)
        traverse_dag_generator = breadth_first_search( self.rev_dag,tasks)
        for task_list in traverse_dag_generator:
            for t in task_list:
                try:
                    t.remove()
                except FileNotFoundError:
                    pass
    
    def remove_orphans(self):
        """Remove all targets for which respective downstream targets are not complete"""

        downstream_complete_dict = {}
        _downstream_complete(self.dag, self.top_nodes, downstream_complete_dict)

        for t, is_downstream_complete in downstream_complete_dict.items():
            if  not is_downstream_complete:
                try:
                    t.remove()
                except FileNotFoundError:
                    pass

    def remove_obsolete(self):
        """Remove all targets whose hash_versions do not match to current version"""
        return

    def update_all_complete_status(self):
        """ Updates a dictionary whose keys are task objects and values are
        True if target was found."""
        from pycarol import Carol, Storage
        st = Storage(Carol())
        target_files = st.files_storage_list(prefix='pipeline/')
        self.all_complete_status = {t: t.output().path in target_files for t in
                self.all_tasks}

    def get_task_complete(self,task):
        """ Returns True if task is complete accordingly to
        self.all_complete_status dictionary.
        When updating many tasks simultaneously, this method is faster the
        standard luigi method."""
        assert task in self.all_tasks, f"Task {task} not found in this pipeline"
        return self.all_complete_status[task]

    def run(self, local_scheduler=True, workers=1, detailed_summary=False):
        """Run the whole pipeline"""
        tasks = [t for t in self.top_nodes]
        return luigi.build(tasks, local_scheduler=local_scheduler,
                           workers=workers, detailed_summary=detailed_summary)

    def get_dag(self):
        return self.dag

    def get_task_by_id(self,task_id):
        """
        Returns the task object given a string task_id.
        This method should be used to implement run_task command line script,
        which will be used in docker tasks and related architectures.
        """
        for t in self.all_tasks:
            if task_id == t.task_id:
                return t
        else:
            raise KeyError(f"{task_id} not found in this pipeline.")

    def get_matching_tasks(self,task):
        """
        Given a non instantiated task, retrieves all instantiated tasks in this pipeline.
        It is used in manual assert in notebook tasks.
        """
        matching_tasks = [isinstance(t, task) for t in self.all_tasks]
        matching_tasks = [t for i, t in enumerate(self.all_tasks) if
                          matching_tasks[i]]
        return matching_tasks


    def assert_task_is_unique(self,task):
        """
        If more than one instance of a class task exist in this pipeline,
        the use of task notebook will be tricky. This assert should be used
        in notebook tasks to warn the user about this situation.
        """
        matching_tasks = self.get_matching_tasks(task)
        assert len(matching_tasks) > 0, \
            f"task {task} not found in this pipeline"
        assert len(matching_tasks) ==1, \
            f"task {task} found multiple times in this pipeline"
        return

    def get_task_instance(self,task):
        """
        This is the method which allows task notebook feature. Given a task
        class it returns corresponding task object in pipeline
        """
        self.assert_task_is_unique(task)
        matching_tasks = self.get_matching_tasks(task)
        return matching_tasks[0]



### Auxiliary functions ###


def _get_dag_(dag, tasks) -> dict:
    """
    Compute the DAQ of a pipeline.


    Args:
        dag: `collections.defaultdict(set)`
            A defaultdict of a set. This will hold the dict with the result {task : requirements_set}
        tasks: `list`
            List of taks to find the dependencies.

    Returns: collections.defaultdict(set)
        the daq as a collections.defaultdict(set)

    """
    for task in tasks:
        if task in dag:
            continue
        #flatten handles dicts and lists.
        reqs = flatten(task.requires())
        dag[task].update(set(reqs))
        if len(reqs) > 0:
            dag = _get_dag_(dag, reqs)
        else:
            continue

    return dag


def _luigi_get_sons(task) -> list:
    """
    Returns a list of required tasks. This is used in build_dag
    Args:
        task: luigi Task

    Returns:
        l: list of luigi Task

    """
    if isinstance(task,tuple):
        # this case happens when we use local params in tasks require
        task, params = task
    return task.requires_list


def _get_dag_from_task(task:list) -> dict:
    """
    Wrapper around generic build_dag.
    Args:
        task: list of proper luigi tasks

    Returns:
        dag: dict encoding a DAG

    """
    dag = build_dag(task, _luigi_get_sons)
    return dag

def _get_instances_from_classes(dag:dict, params:dict):
    """Returns a dag of task instances, given a dag of task classes and pipeline params."""

    dag = {i: list(j) for i,j in dag.items()}
    instances_dag = {}
    for k,v in dag.items():
        task_params = params
        if isinstance(k,tuple):
            k,local_params = k
            task_params.update(local_params)
        task_list=[]
        for t in v:
            task_params = params
            if isinstance(t,tuple):
                t,local_params = t
                task_params.update(local_params)
            task_list.append(t(**task_params))
        instances_dag[k(**params)] = task_list
    return instances_dag

def _downstream_complete(dag, top_nodes, downstream_complete_dict):
    """Recursively traverses dag starting from top_nodes to update downstream_complet_dict"""
    #TODO: reimplement using breadth_first_search
    for task in top_nodes:
        if task in downstream_complete_dict:
            continue
        sons = dag[task]
        if sons: # recursion step
            downstream_complete_dict[task] = task.complete() and \
                all([
                    _downstream_complete(dag, [t], downstream_complete_dict)
                    for t in sons
                    ])
        else: # stop recursion step
            downstream_complete_dict[task] = task.complete()

def _tasks_are_class(tasks):
    for t in tasks:
        if not issubclass(t,Task):
            return False
    else:
        return True

def _tasks_are_instance(tasks):
    for t in tasks:
        if not isinstance(t,Task):
            return False
    else:
        return True



