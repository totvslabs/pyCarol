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

import dis
import importlib
import builtins
import inspect

from pycarol.pipeline.utils import int_to_bytes, flat_list, is_builtin, \
    is_function, is_code, enumerate_with_context

VERBOSE = True  # dev parameter



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


def get_name_and_code_of_MAKE_FUNCTION(ix, inst, instructions):
    """
    When a MAKE_FUNCTION instruction is found, the name and code pointer can
    be found on the two instructions above.
    Args:
        ix: index of MAKE_FUNCTION instruction
        inst: MAKE_FUNCION instruction
        instructions: list of instructions composing the whole bytecode

    Returns: a dict, whose only key is the function name, and value is function code

    """
    assert inst.opname == "MAKE_FUNCTION"
    assert ix >= 2

    name = instructions[ix - 1].argval.split('.')[-1]  # discard context namespace
    code = instructions[ix - 2].argval

    return {name: code}


def find_loaded_function(ix, inst, instructions,local_defs:dict):
    """
    When a LOAD_ATTR instruction is found, it may load a method from an outer
    scope to be used further. The name of the method will be prepended by its
    namespace.
    Args:
        ix: index of LOAD_ATTR instruction
        inst: LOAD_ATTR instruction
        instructions: list of instructions composing the whole bytecode
        local_defs: dict containing all methods and modules visibles at this
        point

    Returns: a dict, whose only key is the function name, and value is function
    code

    """
    assert inst.opname == "LOAD_ATTR"
    assert ix >= 1

    namespace = instructions[ix-1].argval
    function_name = instructions[ix].argval
    name = f"{namespace}.{function_name}"
    try:
        module = local_defs[namespace]
    except KeyError:
        print(local_defs.keys(),namespace, name)
        raise KeyError
    print(module)
    code = getattr(module,function_name)
    assert hasattr(code,'__code__'), code
    code = code.__code__

    return {name: code}


def find_imported(ix, inst, instructions):
    """
    When a IMPORT_NAME instruction is found, we need to update local/global
    context accordingly
    Args:
        ix: index of IMPORT_NAME instruction
        inst: IMPORT_NAME instruction
        instructions: list of instructions composing the whole bytecode

    Returns: a dict, whose only key is the function name, and value is function
    object

    """
    assert inst.opname == "IMPORT_NAME"

    module = instructions[ix].argval
    module_name = instructions[ix+1].argval

    return {module_name: module}


def get_name_of_CALL_FUNCTION(ix, inst, instructions):
    """
    When a CALL_FUNCTION instruction is found, the function name is not given
    as a direct argument to this instruction. Instead, the function name can
    be found some instructions above on the bytecode. Between the CALL_FUNCTION
    instruction and the function pointer we found all the function parameters.
    This method implements the logic needed to fetch the function pointer for
    the three kinds of CALL_FUNCTION operations.

    Args:
        ix: index of call function instruction
        inst: call function instruction
        instructions: list of instructions composing the whole bytecode

    Returns:
        function_name: the name of the called function
    """
    number_of_parameters_in_build_ops = dict(
        BUILD_TUPLE=lambda x: x,
        BUILD_LIST=lambda x: x,
        BUILD_SET=lambda x: x,
        BUILD_MAP=lambda x: 2 * x,
        BUILD_CONST_KEY_MAP=lambda x: x + 1,
        BUILD_STRING=lambda x: x,
        BUILD_TUPLE_UNPACK=lambda x: x,
        BUILD_TUPLE_UNPACK_WITH_CALL=lambda x: x,
        BUILD_LIST_UNPACK=lambda x: x,
        BUILD_SET_UNPACK=lambda x: x,
        BUILD_MAP_UNPACK=lambda x: x,
        BUILD_MAP_UNPACK_WITH_CALL=lambda x: x,
    )

    if "CALL_FUNCTION" == inst.opname:  # it is simple call function instruction
        # for this instruction, we can find the called function some instructions
        # above. we just need to skip backwards the number of arguments
        offset = inst.arg + 1
        # called_function_inst = instructions[ix - offset]
    elif "CALL_FUNCTION_KW" == inst.opname:  # call function op with keyword arguments
        # wrt CALL_FUNCTION there is one additional argument to skip
        offset = inst.arg + 2
        # called_function_inst = instructions[ix - offset]
    elif "CALL_FUNCTION_EX":
        offset = inst.arg + 2
        # Next, we look for BUILD instructions between CALL_FUNCTION_EX instruction
        # and estimated target function position defined by offset. Then, we update
        # offset following each instruction behavior to skip all the parameters
        # and finally find the target function name.
        rev_ind = 0
        while rev_ind < offset:
            rev_ind += 1
            ind_i = ix - rev_ind
            if ind_i < 0:
                raise KeyError("step out of the bytecode while searching CALL_FUNCION_EX target")
            inst_i = instructions[ind_i]
            opname = inst_i.opname
            arg = inst_i.arg
            if opname in number_of_parameters_in_build_ops:  # increase offset
                offset += number_of_parameters_in_build_ops[opname](arg)
    else:
        raise NotImplementedError("instruction {} is not supported".format(inst.opname))

    ix = ix - offset
    called_function_inst = instructions[ix]

    if VERBOSE:
        print(called_function_inst)
        print("offset: ", offset)

    if called_function_inst.opname == 'LOAD_GLOBAL':
        function_name = called_function_inst.argval
    elif called_function_inst.opname == 'LOAD_FAST':
        function_name = called_function_inst.argval
    elif called_function_inst.opname == 'LOAD_ATTR': # TODO: change this
        function_name = instructions[ix].argval
        function_namespace = instructions[ix - 1].argval
        function_name = '.'.join([function_namespace, function_name])
    else:
        raise NotImplementedError(
            f"Composed function name not supported."
            f"{called_function_inst.opname}")
    return function_name


def get_function_object_by_name(
        function_name: str,
        enclosing_function, # function or function code
        local_functions: dict
):
    """
    function_name can be found in Local, Enclosed, Global and Builtin Namespace
    Local and Enclosed namespaces cannot be exactly inferred in static analysis.
    For estimating local and enclosed namespaces, we do not consider conditional
    branches(jumps) in the bytecode. All functions definitions encountered are
    evaluated in the order they appear in the bytecode.

    Args:
        function_name: name of called function
        enclosing_function (function object or bytecode): context function in which
        call function happens
        local_functions: dict containing functions defined in enclosing_function

    Returns:
        func: function object
    """

    if function_name in local_functions:  # Local Namespace
        return local_functions[function_name]

    if hasattr(enclosing_function, '__module__'):
        module_namespace = importlib.import_module(enclosing_function.__module__)
        if hasattr(module_namespace, function_name):  # Module (Global) Namespace
            return getattr(module_namespace, function_name)

    if hasattr(builtins, function_name):  # Builtin Namespace
        return getattr(builtins, function_name)
    # else
    raise NotImplementedError("{} was neither found in module {} nor it is builtin.\
    \nEnclosed namespaces are not supported".format(function_name, module_namespace))


def get_name_and_object_of_CALL_FUNCTION(
        ix,inst,instructions,
        parent_function,defined_functions
) -> tuple:
    """
    Wrapper around get_name_of_CALL_FUNCTION and get_function_object_by_name
    Args:
        ix:
        inst:
        instructions:
        parent_function:
        defined_functions:

    Returns:
        tuple function name, function object

    """
    function_name = get_name_of_CALL_FUNCTION(ix,inst,instructions)
    function_object = get_function_object_by_name(
        function_name,
        parent_function,
        defined_functions
    )
    return function_name, function_object


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
            defined_functions.update(find_imported(*context))

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
