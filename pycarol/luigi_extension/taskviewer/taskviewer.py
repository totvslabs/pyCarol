from pycarol.luigi_extension.utils import build_dag

def luigi_get_sons(task) -> list:
    """
    Returns a list of required tasks. This is used in build_dag
    Args:
        task: luigi Task

    Returns:
        l: list of luigi Task

    """
    return task.requires()


def find_root_in_dag(dag: dict) -> list:
    """
    Search in a direct acyclic graph all nodes without incoming edges
    Args:
        dag: dictionary encoding a DAG

    Returns:
        root_nodes: list of root nodes
    """

def get_dag_node_level(dag: dict, ) -> dict:
    """
    Returns a dict, whose keys are nodes found in dag and values are the
    depth of the node. Root nodes have value 0.
    Args:
        dag: dictionary encoding a DAG

    Returns:
        levels: dict
    """
    root_nodes = find_root_in_dag(dag)
    self.dag_node_level[node] = max(level, self.dag_node_level[node])
    if node not in self.dag:
        return
    for n_i in self.dag[node]:
        self.set_dag_node_level(n_i, level + 1)
