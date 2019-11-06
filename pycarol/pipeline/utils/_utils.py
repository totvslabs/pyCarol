
import builtins

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

    def build_tree(task_list):
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
            build_tree(sons_list)

    build_tree(top_nodes)
    return dag


def find_root_in_dag(dag: dict) -> list:
    """
    Search in a direct acyclic graph all nodes without incoming edges
    Args:
        dag: dictionary encoding a DAG

    Returns:
        root_nodes: list of root nodes
    """
    candidates = [k for k in dag]
    for k, sons_list in dag.items():
        for it in sons_list:
            if it in candidates:
                candidates.remove(it)
    root_nodes = candidates
    return root_nodes

def find_leaf_in_dag(dag: dict) -> list:
    """
    Search in a direct acyclic graph all nodes without outcoming edges
    Args:
        dag: dictionary encoding a DAG

    Returns:
        root_nodes: list of root nodes
    """
    rev_dag = get_reverse_dag(dag)
    return find_root_in_dag(rev_dag)

def get_dag_node_level(dag: dict, ) -> dict:
    """
    Returns a dict, whose keys are nodes found in dag and values are the
    depth of the node. Root nodes have value 0.
    Args:
        dag: dictionary encoding a DAG

    Returns:
        levels: dict
    """
    # levels are initialized to 0.
    # all unreachable nodes are considered to be root nodes
    levels = {}
    for k in dag:
        levels[k] = 0

    for level, nodes_list in enumerate(breadth_first_search(dag)):
        for node in nodes_list:
            levels[node] = level

    return levels


def breadth_first_search(dag: dict, starting_nodes = [], f: 'function'=None,
                         *args,
                         **kwargs):
    """
    Generator that traverse the DAG in breadth first search mode starting in
    given nodes. At each node, execute the function f(node,*args,**kwargs).
    After executing the function, it returns a list of nodes of the current
    depth.
    Args:
        dag: dict encoding the DAG
        starting_nodes: list of starting nodes. if empty all root nodes are
        used.
        f: function to be executed at each node. it receives at least one
        argument

    Returns:
        None
    """
    if not starting_nodes:
        starting_nodes = find_root_in_dag(dag)
    yield starting_nodes

    while starting_nodes:
        if f is not None:
            # visit nodes
            for n in starting_nodes:
                f(n, *args, **kwargs)

        # get all nodes of this level
        sons_list = [n for k in starting_nodes for n in dag[k]]

        yield sons_list

        starting_nodes = sons_list


def get_reverse_dag(dag: dict) -> dict:
    """
    Returns a DAG with the same nodes as the original one, but with edges
    direction reversed. Root nodes become leaf nodes and vice-versa.
    Args:
        dag: dict encoding a DAG

    Returns:
        rev_dag: dict encoding a DAG
    """
    rev_dag = {}
    for source, v in dag.items():
        if source not in rev_dag:
            rev_dag[source] = []
        for target in v:
            if target in rev_dag:
                rev_dag[target].append(source)
            else:
                rev_dag[target] = [source]

    return rev_dag


def is_builtin(f: 'function') -> bool:
    """Returns True if f is built-in"""
    return hasattr(f, '__name__') and hasattr(builtins, f.__name__)


def is_function(f) -> bool:
    """Returns True if f is function object"""
    return hasattr(f, '__code__')


def is_code(c) -> bool:
    return hasattr(c, 'co_code')


def test_isbuiltin():
    assert is_builtin(print)
    def f():
        return None
    assert not is_builtin(f)
    from numpy.fft.fftpack import fft
    assert not is_builtin(fft)


def enumerate_with_context(instructions):
    for ix, inst in enumerate(instructions):
        yield (ix,inst,instructions)