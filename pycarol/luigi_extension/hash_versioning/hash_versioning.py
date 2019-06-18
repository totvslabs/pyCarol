import dis
import importlib
import builtins
import inspect

from ..utils import int_to_bytes, flat_list

VERBOSE = False  # dev parameter


def get_consts_hash(f) -> bytes:
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


def _find_called_function(ix, inst, instructions):
    """
    When a CALL_FUNCTION instruction is found, the function name is not given
    as a direct argument to this instruction. Instead, the function name can
    be found some instructions above on the bytecode. Between the CALL_FUNCTION
    instruction and the function pointer we found all the function parameters.
    This method implements the logic needed to fetch the function pointer for
    the three kind of CALL_FUNCTION operations.

    Args:
        ix: index of call function instruction
        inst: call function instruction
        instructions: list of instructions composing the whole bytecode

    Returns:
        function_name: the name of the called function
    """
    if "CALL_FUNCTION" == inst.opname:  # it is simple call function instruction
        # for this instruction, we can find the called function some instructions
        # above. we just need to skip backwards the number of arguments
        offset = inst.arg + 1
        called_function_inst = instructions[ix - offset]
    elif "CALL_FUNCTION_KW" == inst.opname:  # call function op with keyword arguments
        # wrt CALL_FUNCTION there is one additional argument to skip
        offset = inst.arg + 2
        called_function_inst = instructions[ix - offset]
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
        called_function_inst = instructions[ix - offset]
    else:
        raise NotImplementedError("instruction {} is not supported".format(inst.opname))
    if VERBOSE:
        print(called_function_inst)
        print("offset: ", offset)

    function_name = called_function_inst.argval
    return function_name


def _find_defined_function(ix, inst, instructions):
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


def _fetch_function_object(function_name: str, enclosing_function, local_functions: dict):
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
    \nLocal and Enclosed namespaces are not supported".format(function_name, module_namespace))


def get_bytecode_tree(top_function: 'function', ignore_not_found_function=False) -> list:
    """

    Args:
        top_function:
        ignore_not_found_function:

    Returns:

    """

    def _traverse_code(parent_function: 'function') -> list:

        if hasattr(parent_function, '__name__'):
            if hasattr(builtins, parent_function.__name__):
                # dis cannot get instructions for builtins
                # return function name instead of bytecode
                return [parent_function.__name__]

        nonlocal code_set
        called_functions = set()
        locally_defined_functions: dict = {}
        instructions = list(dis.get_instructions(parent_function))

        for ix, inst in enumerate(instructions):
            context = (ix, inst, instructions)

            if inst.opname == "MAKE_FUNCTION":  # locally defined function
                locally_defined_functions.update(_find_defined_function(*context))

            if "CALL_FUNCTION" in inst.opname:  # call_function op found
                son_function_name = _find_called_function(*context)
                try:
                    son_function = _fetch_function_object(
                        son_function_name, parent_function, locally_defined_functions
                    )
                except NotImplementedError as e:
                    if ignore_not_found_function:
                        continue
                    else:
                        raise e
                if son_function not in code_set:
                    code_set.add(son_function)
                    called_functions.add(son_function)

        code_list = [
            _traverse_code(f) for f in called_functions
            # if f not in code_set  # analyze each function only once
        ]

        if hasattr(parent_function, '__code__'):  # parent_function is function object
            consts = parent_function.__code__.co_consts


            function_code: list = b''.join([
                parent_function.__code__.co_code,
                get_consts_hash(parent_function),
                int_to_bytes(hash(
                    dict(inspect.getmembers(parent_function))['__defaults__']
                )),
            ])
        elif hasattr(parent_function, 'co_code'):  # parent_function is code object
            function_code: list = b''.join([
                parent_function.co_code,
                get_consts_hash(parent_function),
                # missing default parameter information
                # asbytes(hash(
                #     dict(inspect.getmembers(parent_function))['__defaults__']
                # )),
            ])
        else:
            raise TypeError("parent_function should be either function or code.")

        code_list.append(function_code)
        return code_list

    if not hasattr(top_function, '__code__'):
        raise TypeError('argument should be a function')

    code_set = set()  # memoization set
    bytecode_tree = _traverse_code(top_function)
    assert isinstance(bytecode_tree, list)
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
