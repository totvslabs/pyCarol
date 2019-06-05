
from pytest import mark
from ..hash_versioning import get_code_hash



def a(x):
    return x + 5
def b(z):
    return z + 5
def c(x):
    return x - 5

equal_functions_list = [
    (a,a),
    (b,a),
    (b,a),
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
