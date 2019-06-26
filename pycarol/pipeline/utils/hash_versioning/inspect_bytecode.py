"""Functions to inspect bytecodes"""
import importlib
import builtins

from pycarol.pipeline.utils.hash_versioning.hash_versioning import VERBOSE


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


def get_name_and_object_of_LOAD_ATTR(ix, inst, instructions, local_defs:dict):
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


def get_name_and_object_of_IMPORT_NAME(ix, inst, instructions):
    """
    When a IMPORT_NAME instruction is found, we can fetch the name of the
    package and its object in this instruction and the next instruction.
    Args:
        ix: index of IMPORT_NAME instruction
        inst: IMPORT_NAME instruction
        instructions: list of instructions composing the whole bytecode

    Returns: a dict, whose only key is the module name, and value is module
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

    if  hasattr(builtins,function_name):#
        return getattr(builtins, function_name)

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