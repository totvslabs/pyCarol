from . import *

dag={
    0: [1,2],
    1: [2,3],
    2: [4],
    3: [4],
    4: []
}
l=[]


def test_breadth_first_search():
    def print_visiting_order(node, visiting_list):
        print(visiting_list,'.',node)
        visiting_list.append(node)

    for i,levels in enumerate(
            breadth_first_search(dag,[0],print_visiting_order,l)):
        print(i,levels)


def test_get_dag_node_level():
    levels = get_dag_node_level(dag)
    print(levels)


def test_get_reverse_dag():
    print(dag)
    print(get_reverse_dag(dag))
    print(get_reverse_dag(get_reverse_dag(dag)))
    assert dag == get_reverse_dag(get_reverse_dag(dag))
