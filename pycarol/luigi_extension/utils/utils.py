def int_to_bytes(i: int) -> bytes:
    """
    Wrapper around int.to_bytes. Set some defaults parameters

    """
    return i.to_bytes(i.bit_length() // 8 + 1, 'little', signed=True)


def flat_list(tree: list) -> list:
    """
    Recursively unnest a nested list
    Args:
        tree: nested lists of unlimited depth

    Returns:
        l: flat list

    """
    l = []
    for node in tree:
        if isinstance(node, list):
            l += flat_list(node)
        else:
            l.append(node)
    return l


def build_dag(top_nodes: list, get_sons: 'function' = None) -> dict:
    """
    Extract a Direct Acyclic Graph structure of a pipeline using
    get_sons_method to fetch sons nodes

    Args:
        top_nodes: list of top tasks/nodes
        get_sons: method to extract sons node (required tasks) of a
        given node. method get_sons should return a list of nodes

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