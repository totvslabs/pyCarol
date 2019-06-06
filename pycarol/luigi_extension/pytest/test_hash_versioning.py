from pytest import mark
from ..hash_versioning import get_bytecode_tree


def a(x):
    return x + 5


def b(z):
    return z + 5


def c(x):
    return x - 5


def d(x):
    return x + 4


def a_of_a(x):
    return a(x)


def a_of_b(x):
    return b(x)


def eternal_recursion_a(x):
    return eternal_recursion_a(x)


def eternal_recursion_b(x):
    return eternal_recursion_b(x)


def mutual_recursion_a(x):
    return mutual_recursion_b(x)


def mutual_recursion_b(x):
    return mutual_recursion_a(x)


def initial_value_a(x=3):
    return x + 5


def initial_value_b(x=5):
    return x + 5


def outer_initial_value_a():
    return initial_value_a()


def outer_initial_value_b():
    return initial_value_b()


def nested_a():
    def _nested():
        return (0)

    return _nested()


def nested_b():
    def _nested():
        return (100)

    return _nested()


def nested_c():
    def _nested():
        return (100)

    return _nested()


def variable_args_a(*args):
    print(args)


def variable_args_b(*args):
    print(args)


equal_functions_list = [
    (a, a),
    (b, a),
    (a, b),
    (a_of_a, a_of_b),
    (eternal_recursion_a, eternal_recursion_b),
    (mutual_recursion_a, mutual_recursion_b),
    (variable_args_a, variable_args_b),
    (nested_b, nested_c),
]

different_functions_list = [
    (a, c),
    (b, c),
    (a, d),
    (mutual_recursion_a, eternal_recursion_a),
    (initial_value_a, a),
    (initial_value_a, initial_value_b),
    (outer_initial_value_a, outer_initial_value_b),
    (nested_a, nested_b),
]


@mark.parametrize("f1,f2", equal_functions_list)
def test_equal_functions(f1, f2):
    assert get_bytecode_tree(f1) == get_bytecode_tree(f2)


@mark.parametrize("f1,f2", different_functions_list)
def test_different_functions(f1, f2):
    assert get_bytecode_tree(f1) != get_bytecode_tree(f2)
