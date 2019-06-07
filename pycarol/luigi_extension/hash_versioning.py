import dis
import importlib
import builtins
import inspect

VERBOSE = True

def asbytes(i: int) -> bytes:
    return i.to_bytes(i.bit_length() // 8 + 1, 'little', signed=True)


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
        if VERBOSE:
            print(called_function_inst, offset)
    else:
        raise NotImplementedError("instruction {} is not supported".format(inst.opname))

    function_name = called_function_inst.argval
    return function_name


def _fetch_function_object(function_name, enclosing_function):
    """
    function_name can be found in Local, Enclosed, Global and Builtin Namespace
    Local and enclosed namespaces cannot be discovered in static analysis.
    So there is limited support to nested functions like this one.

    Args:
        function_name: string
        enclosing_function: function object

    Returns:
        func: function object
    """

    module_namespace = importlib.import_module(enclosing_function.__module__)
    if hasattr(module_namespace, function_name):
        func = getattr(module_namespace, function_name)  # Module (Global) Namespace
    elif hasattr(builtins, function_name):
        func = getattr(builtins, function_name)  # Builtin Namespace
    else:
        raise NotImplementedError("{} was neither found in module {} nor it is builtin.\
        \nLocal and Enclosed namespaces are not supported".format(function_name, module_namespace))
    return func


def get_bytecode_tree(analyzed_function: 'function', ignore_not_found_function=True) -> list:
    """

    Args:
        analyzed_function:
        ignore_not_found_function:

    Returns:

    """

    def _traverse_code(enclosed_function: 'function') -> list:
        nonlocal code_set
        inner_functions_set = set()
        if hasattr(builtins, enclosed_function.__name__):
            # dis cannot get instructions for builtins
            # return function name instead of bytecode
            return [enclosed_function.__name__]
        instructions = list(dis.get_instructions(enclosed_function))

        for ix, inst in enumerate(instructions):
            if "CALL_FUNCTION" in inst.opname:  # call_function op found
                function_name = _find_called_function(ix, inst, instructions)
                try:
                    func = _fetch_function_object(function_name, enclosed_function)
                except NotImplementedError as e:
                    if ignore_not_found_function:
                        continue
                    else:
                        raise e

                if func not in code_set:
                    code_set.add(func)
                    inner_functions_set.add(func)

        code_list = [_traverse_code(f) for f in inner_functions_set]
        function_code = b''.join([
            enclosed_function.__code__.co_code,
            asbytes(hash(
                enclosed_function.__code__.co_consts
            )),
            asbytes(hash(
                dict(inspect.getmembers(enclosed_function))['__defaults__']
            )),
        ])

        code_list.append(function_code)
        return code_list

    if not hasattr(analyzed_function, '__code__'):
        raise TypeError('argument should be a function')

    code_set = set()  # memoization set
    bytecode_tree = _traverse_code(analyzed_function)
    assert isinstance(bytecode_tree, list)
    return bytecode_tree

# TODO: support MAKE_FUNCTION. it should allow nested functions
# TODO: support inner imports. possible?
