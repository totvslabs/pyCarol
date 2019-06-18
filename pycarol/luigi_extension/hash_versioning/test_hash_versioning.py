from pytest import mark
from pycarol.luigi_extension.hash_versioning import get_bytecode_tree, _find_called_function


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


def nested_d():
    def _nested(x=5, y=80):
        print(kw)
        return (100 + x + y)

    return _nested(y=0, x=7)

def variable_args_a(*args):
    return (args)


def variable_args_b(*args):
    return (args)


def variable_args_c(*args):
    return args[::-1]


def builtin_a():
    print('hey')


def builtin_b():
    print('hey')


def builtin_c():
    print('hi')


def builtin_d():
    print('hey', 'hi')


def dummy_function():
    pass


def call_kwargs_a():
    return dummy_function(0, p1=10, p2=20)


def call_kwargs_b():
    return dummy_function(0, p1=10, p2=20)


def call_kwargs_c():
    return dummy_function(0, p1=10, p2=50)


def call_kwargs_d():
    return dummy_function(0, p1=10, p3=20)


def call_ex_a(x):
    return dummy_function(*x)


def call_ex_b(x):
    return dummy_function(*x, *x)


def call_ex_c(x):
    return dummy_function(**x)


def call_ex_d(x):
    return dummy_function(**x, **x)


def call_ex_e(x):
    import importlib
    d = importlib.import_module(dis)
    return dummy_function(d, *x)


def call_ex_f(x):
    return dummy_function(0, *x, **x)


def call_ex_g(x):
    return dummy_function(0, dummy_function, **x)


def call_ex_h(x, y):
    return dummy_function(0, 1, 2, [0, 1, 2], ('a', 0), *x, *x, **y, **y, p1={0, 1, 2})


equal_functions_list = [
    (a, a),
    (b, a),
    (a, b),
    (a_of_a, a_of_b),
    (eternal_recursion_a, eternal_recursion_b),
    (mutual_recursion_a, mutual_recursion_b),
    (variable_args_a, variable_args_b),
    (nested_b, nested_c),
    (builtin_a, builtin_b),
    (call_kwargs_a, call_kwargs_b)
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
    (variable_args_a, variable_args_c),
    (builtin_a, builtin_c),
    (builtin_a, builtin_d),
    (call_kwargs_a, call_kwargs_c),
    (call_kwargs_a, call_kwargs_d),
]

calling_functions_list = [
    call_kwargs_a,
    call_kwargs_b,
    call_kwargs_c,
    call_kwargs_d,
    call_ex_a,
    call_ex_b,
    call_ex_c,
    call_ex_d,
    call_ex_e,
    call_ex_f,
    call_ex_g,
    call_ex_h,
]


@mark.parametrize("func", calling_functions_list)
def test_find_called_function(func):
    import dis
    print(dis.dis(func))
    instructions = list(dis.get_instructions(func))
    ix = len(instructions) - 2
    inst = instructions[ix]
    # assert that the func return another function
    assert "CALL_FUNCTION" in inst.opname
    assert _find_called_function(ix, inst, instructions) == "dummy_function"


@mark.parametrize("f1,f2", equal_functions_list)
def test_equal_functions(f1, f2):
    assert get_bytecode_tree(f1) == get_bytecode_tree(f2)


@mark.parametrize("f1,f2", different_functions_list)
def test_different_functions(f1, f2):
    assert get_bytecode_tree(f1) != get_bytecode_tree(f2)
