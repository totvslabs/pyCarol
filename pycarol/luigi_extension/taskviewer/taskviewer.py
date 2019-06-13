import numpy as np
from pycarol.luigi_extension.task import Task

from typing import Tuple

def luigi_get_sons(task) -> list:
    """
    Returns a list of required tasks.
    Args:
        task: luigi Task

    Returns:
        l: list of luigi Task

    """
    return task.requires()

def get_dag_from_task(top_nodes: list, get_sons: 'function' = None) -> dict:
    """
    Extract a Direct Acyclic Graph structure of a pipeline using 
    get_sons_method to fetch sons nodes
    
    Args:
        task: list of top tasks/nodes
        get_sons: method to extract sons node (required tasks) of a 
        given node

    Returns:
        dag: dictionary encoding a DAG data structure.
 
    """
    assert isinstance(top_nodes,list)
    dag = {}

    def _traverse_tree(task_list):
        # breadth first search
        nonlocal dag, get_sons

        # add new nodes
        for t in task_list:
            if t not in dag:
                dag[t] = get_sons(t)

        # get all nodes of this level
        sons_list = []
        for k,v in dag.items():
            for vi in v:
                if vi not in dag:
                    sons_list.append(vi)

        # recursion level wise
        if sons_list:
            _traverse_tree(sons_list)

    _traverse_tree(top_nodes)
    return dag


