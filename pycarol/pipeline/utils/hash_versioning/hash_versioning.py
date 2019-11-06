

import dis
import inspect

from pycarol.pipeline.utils import (
    int_to_bytes,
    flat_list,
    is_builtin,
    is_function,
    is_code,
    enumerate_with_context
)

#TODO: move this module to a dedicated repository
DEBUG_MODE = False

from pycarol.pipeline.utils.hash_versioning.inspect_bytecode import (
    get_name_and_code_of_MAKE_FUNCTION,
    get_name_and_object_of_IMPORT_NAME,
    get_name_and_object_of_CALL_FUNCTION,
    get_name_and_object_of_LOAD_ATTR,
)


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

def process_op(
        context,
        defined_functions,
        code_set,
        called_functions,
        parent_function,
        robust
        ):
    """
    Process instruction updating defined_functions, code_set and
    called_functions accordingly
    Args:
        context: ix, inst, instructions. bytecode itself
        defined_functions: set of functions defined until now
        code_set: set of functions already inspected for hash versioning purpose
        called_functions: set of called functions until now in this bytecode
        parent_function: function object of this bytecode
        robust: set true to ignore internal assertion errors

    Returns:

    """
    ix, inst, instructions = context
    if inst.opname == "MAKE_FUNCTION":
        defined_functions.update(
            get_name_and_code_of_MAKE_FUNCTION(*context))

    if inst.opname == "IMPORT_NAME": #TODO: some tests are failing for IMPORT_NAME
        defined_functions.update(
            get_name_and_object_of_IMPORT_NAME(*context))

    if "CALL_FUNCTION" in inst.opname:
        try:
            _, son_function = get_name_and_object_of_CALL_FUNCTION(
                ix,inst,instructions,
                parent_function,
                defined_functions
            )
        except AssertionError as error:
            if robust:
                # print(error)
                return  # do not update code_set and called_functions
            else:
                raise(error)

        if son_function not in code_set:
            code_set.add(son_function)
            called_functions.add(son_function)
    return

def traverse_code(parent_function: 'function', code_set,
                  ignore_not_implemented=True) -> list:
    """
    This method recursively traverse the bytecodes of top_function entering
    in every function call found. At every level it returns a list of
    bytecodes of respective functions called.

    Args:
        parent_function: function to be analyzed at this recursion level
        code_set: set of functions already analyzed
        ignore_not_implemented: if True, will generate bytecode_tree where it is
        possible. Where it is not possible, will return empty list.

    Returns:
        bytecode_tree: nested lists of bytecode

    """

    if is_builtin(parent_function):
        # dis cannot get instructions for builtins
        # return function name instead of bytecode
        return [parent_function.__name__]

    called_functions = set()
    defined_functions: dict = {}

    # linear search on bytecodes for called functions
    instructions = list(dis.get_instructions(parent_function))
    for context in enumerate_with_context(instructions):
        try:
            process_op(context,defined_functions,code_set,called_functions,
                       parent_function,ignore_not_implemented)
        except NotImplementedError as e:
            if ignore_not_implemented:
                continue
            else:
                raise e

    # recursion step
    code_list = [traverse_code(f, code_set, ignore_not_implemented) for f in
                 called_functions]

    # leaf node step
    if is_function(parent_function):

        function_code: list = b''.join([
            parent_function.__code__.co_code,
            _get_consts_hash(parent_function),
            int_to_bytes(hash(
                dict(inspect.getmembers(parent_function)).get('__defaults__',0)
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


def get_bytecode_tree(
        top_function: 'function',
        ignore_not_implemented=False,
) -> list:
    """
    Args:
        top_function: analyzed function
        ignore_not_implemented: if True, will generate bytecode_tree where it is
        possible. Where it is not possible, will return empty list.

    Returns:
        bytecode_tree: nested lists of bytecode

    """
    #TODO: nested, builtin, call_kwargs are failing interprocesses test
    from functools import partial
    if isinstance(top_function,partial):
        top_function = top_function.func

    assert is_function(top_function), 'argument should be a function'
    code_set = set()  # "global" memoization set
    bytecode_tree = traverse_code(top_function, code_set,ignore_not_implemented)

    return bytecode_tree

def get_function_hash(f: 'function', ignore_not_implemented=False) -> int:
    """
    Module main function. It returns a proper hash for the given function.
    Args:
        f: function to be hashed
        ignore_not_implemented: if True, will consider bytecode_tree where it is
        possible. Where it is not possible, will replace by an empty list.
        and compute a hash anyway. Some part of the code may be ignored.

    Returns:
        h: a hash number for the given function

    """
    bytecode_nested_list = get_bytecode_tree(f, ignore_not_implemented)
    bytecode_flat_list = flat_list(bytecode_nested_list)
    bytecode_flat_list = [
        bytecode.encode()
        if isinstance(bytecode,str)
        else bytecode
        for bytecode in bytecode_flat_list
    ]
    
    from hashlib import sha256
    hashable_bytecode = b''.join(bytecode_flat_list)
    h = sha256(hashable_bytecode).hexdigest()
    return h


