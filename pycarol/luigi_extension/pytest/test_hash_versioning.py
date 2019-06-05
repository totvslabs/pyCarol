
from pytest import mark
from ..hash_versioning import get_code_hash


def a(x):
    return x + 5
def b(z):
    return z + 5
def c(x):
    return x - 5

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


equal_functions_list = [
    (a,a),
    (b,a),
    (a,b),
    (a_of_a,a_of_b),
    (eternal_recursion_a,eternal_recursion_b),
    (mutual_recursion_a,mutual_recursion_b),
]

different_functions_list = [
    (a,c),
    (b,c),
]

@mark.parametrize("f1,f2",equal_functions_list)
def test_equal_functions(f1,f2):
    assert get_code_hash(f1) == get_code_hash(f2)

@mark.parametrize("f1,f2",different_functions_list)
def test_different_functions(f1,f2):
    assert get_code_hash(f1) != get_code_hash(f2)
