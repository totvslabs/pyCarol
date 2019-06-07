import dis
import importlib
import builtins
import inspect


def asbytes(i: int) -> bytes:
    return i.to_bytes(i.bit_length() // 8 + 1, 'little', signed=True)


def get_bytecode_tree(analyzed_function: 'function', ignore_not_found_function=True) -> list:
    def _traverse_code(_analyzed_function: 'function') -> list:
        nonlocal code_set
        inner_functions_set = set()
        print(_analyzed_function.__name__)
        if hasattr(builtins, _analyzed_function.__name__):
            # dis cannot get instructions for builtins
            # return function name instead of bytecode
            return [_analyzed_function.__name__]
        instructions = list(dis.get_instructions(_analyzed_function))

        m = _analyzed_function.__module__
        imported = importlib.import_module(m)
        for ix, inst in enumerate(instructions):
            if "CALL_FUNCTION" in inst.opname:  # call_function op found
                if "CALL_FUNCTION" == inst.opname:  # it is simple call function instruction
                    # for this instruction, we can find the called function some instructions
                    # above. we just need to skip backwards the number of arguments
                    called_function_name = instructions[ix - inst.arg - 1]
                elif "CALL_FUNCTION_KW" == inst.opname:  # call function op with keyword arguments
                    # wrt CALL_FUNCTION there is one additional argument to skip
                    called_function_name = instructions[ix - inst.arg - 2]
                elif "CALL_FUNCTION_EX":
                    raise NotImplementedError("instruction {} is not supported".format(inst.opname))
                else:
                    raise NotImplementedError("instruction {} is not supported".format(inst.opname))
                function_name = called_function_name.argval
                # function_name can be found in Local, Enclosed, Global and Builtin Namespace
                # Local and enclosed namespaces cannot be discovered in static analysis.
                # So there is limited support to nested functions like this one.
                if hasattr(imported, function_name):
                    func = getattr(imported, function_name)  # Module (Global) Namespace
                elif hasattr(builtins, function_name):
                    func = getattr(builtins, function_name)  # Builtin Namespace
                else:
                    if ignore_not_found_function:
                        continue
                    else:
                        raise NotImplementedError("{} was neither found in module {} nor it is builtin.\
                        \nLocal and Enclosed namespaces are not supported".format(function_name, imported))

                if func not in code_set:
                    code_set.add(func)
                    inner_functions_set.add(func)
        code_list = [_traverse_code(f) for f in inner_functions_set]
        function_code = b''.join([
            _analyzed_function.__code__.co_code,
            asbytes(hash(
                _analyzed_function.__code__.co_consts
            )),
            asbytes(hash(
                dict(inspect.getmembers(_analyzed_function))['__defaults__']
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

# TODO: support CALL_FUNCTION_EX
# TODO: support MAKE_FUNCTION. it should allow nested functions
# TODO: support inner imports. possible?
