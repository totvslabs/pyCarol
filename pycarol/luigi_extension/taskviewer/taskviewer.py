import numpy as np
from pycarol.luigi_extension.task import Task

from typing import Tuple


def get_dag_from_task(task_list: list) -> Tuple[dict, list]:
    """
    Args:
        task: list of luigi task instance with parameters defined

    Returns:
        dag: dictionary encoding a DAG data structure. nodes in this DAG are
        integers.
        nodes_list: list of tasks. the nodes numbers in dag should be used as
        indexes of this list.
    """
    if isinstance(task_list,Task):
        task_list = [task_list]

    if not isinstance(task_list,list):
        raise TypeError
    dag = {}
    def _traverse_tree(task_list):
        # breadth first search
        nonlocal dag

        # add new nodes
        for t in task_list:
            if t not in dag:
                dag[t] = t.requires()

        # get all nodes of this level
        sons_list = []
        for k,v in dag.items():
            for vi in v:
                if vi not in dag:
                    sons_list.append(vi)

        # recursion level wise
        if sons_list:
            _traverse_tree(sons_list)
    _traverse_tree(task_list)
    return dag


