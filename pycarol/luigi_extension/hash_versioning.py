import dis
import importlib
import builtins


def get_bytecode_tree(analyzed_function):
    def _traverse_code(_analyzed_function):
        nonlocal code_set
        inner_functions_set = set()
        if hasattr(builtins, _analyzed_function.__name__):
            # dis cannot get instructions for builtins
            # return function name instead of bytecode
            return [_analyzed_function.__name__]
        instructions = list(dis.get_instructions(_analyzed_function))

        m = _analyzed_function.__module__
        imported = importlib.import_module(m)
        for ix, inst in enumerate(instructions):
            if "CALL_FUNCTION" in inst.opname:  # call function instruction found
                if "CALL_FUNCTION" == inst.opname:  # it is simple call function instruction
                    # for this instruction, we can find the called function some instructions
                    # above. we just need to skip bakcwards the number of arguments
                    called_function_name = instructions[ix - inst.arg - 1]
                else:
                    raise (NotImplementedError, "{} instruction is not supported".format(inst.opname))
                function_name = called_function_name.argval
                # function_name can be found in Local, Enclosed, Global and Builtin Namespace
                # Local and enclosed namespaces cannot be discovered in static analysis.
                # So there is limited support to nested functions like this one.
                try:
                    func = getattr(imported, function_name)  # Module (Global) Namespace
                except AttributeError:
                    func = getattr(builtins, function_name)  # Builtin Namespace

                if func not in code_set:
                    code_set.add(func)
                    inner_functions_set.add(func)
        code_list = [_traverse_code(f) for f in inner_functions_set]
        code_list.append(_analyzed_function.__code__.co_code)
        return code_list

    if not hasattr(analyzed_function, '__code__'):
        raise TypeError('argument should be a function')

    code_set = set()  # memoization set
    bytecode_tree = _traverse_code(analyzed_function)
    assert isinstance(bytecode_tree, list)
    return bytecode_tree
