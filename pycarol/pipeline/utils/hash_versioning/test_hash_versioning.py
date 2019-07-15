from pytest import mark
from . import get_bytecode_tree, get_function_hash
from pycarol.pipeline.utils.hash_versioning import get_name_of_CALL_FUNCTION


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
        print('kw')
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
    d = importlib.import_module('dis')
    return dummy_function(d, *x)


def call_ex_f(x):
    return dummy_function(0, *x, **x)


def call_ex_g(x):
    return dummy_function(0, dummy_function, **x)


def call_ex_h(x, y):
    return dummy_function(0, 1, 2, [0, 1, 2], ('a', 0), *x, *x, **y, **y, p1={0, 1, 2})

import pandas as pd
import pandas
def external_import_a(x):
    return pandas.Series.cumsum(x)

def external_import_b(x):
    return pandas.Series.sum(x)

def internal_import_a(x):
    import pandas as pd
    return pd.Series(x)

def internal_import_b(x):
    import pandas as pd
    return pd.DataFrame(x)

def pick_import_a(x):
    from pandas import Series
    return Series(x)

def pick_import_b(x):
    from pandas import DataFrame
    return DataFrame(x)

from numpy.fft import fft as f1
from numpy.fft import fft2 as f2
def pick_import_c(x):
    return f1(x)

def pick_import_d(x):
    return f2(x)

from pandas import Series
def pick_import_e(x):
    return Series.cumsum(x)

def pick_import_f(x):
    return Series.sum(x)

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
    (external_import_a,external_import_b),
    (pick_import_a,pick_import_b),
    (pick_import_c, pick_import_d),
    (pick_import_e, pick_import_f),
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
    assert get_name_of_CALL_FUNCTION(ix, inst, instructions,func
                                     ) == "dummy_function"


@mark.parametrize("f1,f2", equal_functions_list)
def test_equal_functions(f1, f2):
    assert get_bytecode_tree(f1) == get_bytecode_tree(f2)


@mark.parametrize("f1,f2", different_functions_list)
def test_different_functions_robust(f1, f2):
    assert get_bytecode_tree(f1,ignore_not_implemented=True) != \
           get_bytecode_tree(f2,ignore_not_implemented=True)



all_functions = set()
for a,b in equal_functions_list + different_functions_list:
    all_functions.add(a)
    all_functions.add(b)
all_functions = [f for f in all_functions]

@mark.parametrize("f",all_functions)
def test_generate_bytecode(f):
    assert get_bytecode_tree(f,ignore_not_implemented=True)


@mark.parametrize("f",all_functions)
def test_generate_hash(f):
    print(get_bytecode_tree(f,ignore_not_implemented=True))
    assert get_function_hash(f,ignore_not_implemented=True)


from joblib import Parallel, delayed

def get_hash(f):
    name = f.__name__
    try:
        bytecode = get_bytecode_tree(f)
    except:
        bytecode = "FAIL"
    try:
        h = get_function_hash(f)
    except:
        h = "FAIL"    
    return([name, bytecode, h])


TDD_tests = False
# We place here tests that are not passing, but should not block a PR
# Typically, they are tests related to WIP
if TDD_tests:
    different_functions_list.append((internal_import_a,internal_import_b),)

    @mark.parametrize("f1,f2", different_functions_list)
    def test_different_functions(f1, f2):
        assert get_bytecode_tree(f1) != get_bytecode_tree(f2)
    @mark.parametrize("f1,f2", different_functions_list)

    def test_different_functions_robust_extended(f1, f2):
        assert get_bytecode_tree(f1,ignore_not_implemented=True) != \
            get_bytecode_tree(f2,ignore_not_implemented=True)

