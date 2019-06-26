"""
One of the main purposes of using pipeline managers like Luigi, Dask, Airflow is
to store the result of heavy computation tasks for further reuse. It happens
often that the process of developing the whole pipeline is very dynamic. The
code of a task can be explicitly changed or the task behavior can change due
to external changes, like an update of a python package. It turns out that
one cannot be certain that the stored result of the task is up-to-date with
the code. This may invalidate any test/validation process.
The solution we propose here is to generate a hash of the code of each task.
In the case of using Luigi, we assume that if the target was generated with the
same task parameters and it has the same hash code we can safely use this
target (of course, excluding hash collision hypothesis). We observe that some
pipeline managers also keep track of the data/inputs hash. This module can
cooperate with these managers as well.
Finally, the practical objective of this module is to obtain a hash code of a
given function statically, i.e., without computing the python code itself.
Due to its statical nature, some case cannot be supported, like import and
functions definitions inside if statements.
The method get_bytecode_tree performs all the hard work and return nested lists
of bytecodes that may be useful for inspection.
The method get_function_hash uses get_bytecode_tree to return a hash for a
given function.
"""
VERBOSE = True  # dev parameter
import dis
import inspect

from pycarol.pipeline.utils import int_to_bytes, flat_list, is_builtin, \
    is_function, is_code, enumerate_with_context
from pycarol.pipeline.utils.hash_versioning.inspect_bytecode import \
    get_name_and_code_of_MAKE_FUNCTION, get_name_and_object_of_IMPORT_NAME, \
    get_name_and_object_of_CALL_FUNCTION





def _get_consts_hash(f) -> bytes:
    """Returns hash code of local consts"""
    if hasattr(f, '__code__'):  # is function object
        consts_list = list(f.__code__.co_consts)
    elif hasattr(f, 'co_code'):  # is code object
        consts_list = list(f.co_consts)
    else:
        raise TypeError

    for i, v in enumerate(consts_list):
        if isinstance(v, str) and "<locals>" in v:
            consts_list[i] = v.split('.')[-1]
    consts_tuple = tuple(consts_list)
    return int_to_bytes(hash(consts_tuple))


def get_bytecode_tree(
        top_function: 'function',
        ignore_not_implemented=False,
) -> list:
    """
    This method recursively traverse the bytecodes of top_function entering
    in every function call found. At every level it returns a list of
    bytecodes of respective functions called. The recursion is implemented by
    local function traverse_code.


    Args:
        top_function: analyzed function
        ignore_not_implemented: if True, will generate bytecode_tree where it is
        possible. Where it is not possible, will return empty list.

    Returns:
        bytecode_tree: nested lists of bytecode

    """
    def process_op(
            context,
            defined_functions,
            code_set,
            called_functions,
            parent_function,
            ):
        ix, inst, instructions = context
        if inst.opname == "MAKE_FUNCTION":  # locally defined function
            defined_functions.update(
                get_name_and_code_of_MAKE_FUNCTION(*context))

        if inst.opname == "IMPORT_NAME":
            defined_functions.update(
                get_name_and_object_of_IMPORT_NAME(*context))

        if "CALL_FUNCTION" in inst.opname:
            _, son_function = get_name_and_object_of_CALL_FUNCTION(
                ix,inst,instructions,
                parent_function,
                defined_functions
            )

            if son_function not in code_set:
                code_set.add(son_function)
                called_functions.add(son_function)


    def traverse_code(parent_function: 'function') -> list:

        if is_builtin(parent_function):
            # dis cannot get instructions for builtins
            # return function name instead of bytecode
            return [parent_function.__name__]

        nonlocal code_set
        called_functions = set()
        defined_functions: dict = {}

        instructions = list(dis.get_instructions(parent_function))
        for context in enumerate_with_context(instructions):
            process_op(context,defined_functions,code_set,called_functions,parent_function)

        # recursion step
        code_list = [ traverse_code(f) for f in called_functions ]

        # leaf node step
        if is_function(parent_function):
            function_code: list = b''.join([
                parent_function.__code__.co_code,
                _get_consts_hash(parent_function),
                int_to_bytes(hash(
                    dict(inspect.getmembers(parent_function))['__defaults__']
                )),
            ])
        elif is_code(parent_function):  # parent_function is code object
            function_code: list = b''.join([
                parent_function.co_code,
                _get_consts_hash(parent_function),
                # missing default parameter information
                # asbytes(hash(
                #     dict(inspect.getmembers(parent_function))['__defaults__']
                # )),
            ])
        else:
            raise TypeError("parent_function should be either function or code.")

        code_list.append(function_code)
        return code_list

    ###get_bytecode_tree###
    assert is_function(top_function), 'argument should be a function'

    code_set = set()  # "global" memoization set

    bytecode_tree = traverse_code(top_function)

    return bytecode_tree


def get_function_hash(f: 'function', ignore_not_found_function=False) -> int:
    """
    Module main function. It returns a proper hash for the given function.
    Args:
        f: function to be hashed
        ignore_not_found_function: setting to True will ignore some errors
        and compute a hash anyway. Some part of the code may be ignored.

    Returns:
        h: a hash number for the given function

    """
    bytecode_nested_list = get_bytecode_tree(f, ignore_not_found_function)
    bytecode_flat_list = flat_list(bytecode_nested_list)
    h = hash(tuple(bytecode_flat_list))
    return h




# TODO: improve documentation
# TODO: real scenario test case
# TODO: support inner imports. possible?
