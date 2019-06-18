from ..utils import get_reverse_dag, breadth_first_search


def nodes_layout(dag:dict, align_on_leafs = True) -> dict:
    """
    Builds basic graph plot layout. In this version, nodes are placed on x
    axis accordingly to their depth in dag. Top levels are on left whereas
    deeper nodes are placed on the right. If align on_leafs is set to true,
    DAG is first reversed, so that the output of the pipeline is on the right
    and inputs on the left.
    Args:
        dag: dict encoding a DAG structure
        align_on_leafs: layout boolean parameter

    Returns:
        layout: a dict whose keys are DAG nodes an values are (x,y) of each
        node.

    """

    layout = {}
    if align_on_leafs:
        dag = get_reverse_dag(dag)
    for i, nodes in enumerate(breadth_first_search(dag)):
        for j, node in enumerate(nodes):
            layout[node] = (i,j)
    return layout

def edges_layout(dag:dict, layout:dict) -> list:
    """
    Given a dag network and the positions of each one of its nodes,
    this function creates the positions of the edges.
    Args:
        dag: dict encoding a DAG
        layout: dict containing nodes x,y positions

    Returns:
        edges: list of positions, each one in format ( (x0,y0), (x1,y1) )
    """

    edges = []
    for source_node in layout:
        for target_node in dag[source_node]:
            edges.append((layout[source_node],layout[target_node]),)
    return edges

def _get_task_id(t):
    return t.task_id

def _get_task_family(t):
    if '.' in t.task_id:
        return t.task_id.split('.')[-2]
    else:
        return "empty_namespace"

def _get_task_name(t):
    return t.task_id.split('.')[-1]

def _get_complete(t):
    return t.complete()

def _get_tasklog(t):
    log = t.loadlog()
    if isinstance(log, str):
        return log
    else:
        return "Log type is wrong."

def _get_hash_version(t):
    return ""


def make_nodes_data_source(nodes_layout) -> dict:
    """
    Creates a bokeh compatible data source encoding nodes plotting
    properties. Returns this data source in bokeh compatible dict format
    Args:
        nodes_layout: dict containing nodes as keys and its positions as
        values. In this function nodes are luigi tasks.

    Returns:
        data_source: bokeh compatible dict containing the columns: x, y,
        task_id, task_family, task_name, complete, tasklog, hash_version

    """

    data = dict(
        # task=[],
        x=[],
        y=[],
        task_id=[],
        task_family=[],
        task_name=[],
        complete=[],
        tasklog=[],
        hash_version=[],
    )
    for k,(x,y) in nodes_layout.items():
        # data['task'].append(k)
        data['x'].append(x)
        data['y'].append(y)
        data['task_id'].append(_get_task_id(k))
        data['task_family'].append(_get_task_family(k))
        data['task_name'].append(_get_task_name(k))
        data['complete'].append(_get_complete(k))
        data['tasklog'].append(_get_tasklog(k))
        data['hash_version'].append(_get_hash_version(k))

    return data

def make_edges_data_source(edges_layout) -> dict:
    """
    Creates a bokeh segment glyph compatible data source encoding edges plotting
    properties. Returns this data source in bokeh compatible dict format
    Args:
        edges_layout: list containing edges coordinates.

    Returns:
        data_source: bokeh segment glyph compatible dict containing the
        columns: x0, y0, x1, y1

    """

    data = dict(
        x0=[],
        y0=[],
        x1=[],
        y1=[]
    )
    for ((x0,y0,),(x1,y1)) in edges_layout:
        data['x0'].append(x0)
        data['y0'].append(y0)
        data['x1'].append(x1)
        data['y1'].append(y1)

    return data

