import dis
import importlib

def _traverse_code(analyzed_function):
    inner_functions_set = set()
    instructions = list(dis.get_instructions(analyzed_function))
    m = analyzed_function.__module__
    imported = importlib.import_module(m)
    for ix, inst in enumerate(instructions):
        if 'CALL_FUNCTION' == inst.opname:
            called_function_name = instructions[ix - inst.arg - 1]
            function_name = called_function_name.argval
            func = getattr(imported, function_name)
            print(function_name,func)
            if func not in code_set:
                code_set.add(func)
                inner_functions_set.add(func)
    code_list = [_traverse_code(f) for f in inner_functions_set]
    code_list.append(analyzed_function.__code__.co_code)
    if len(code_list) == 1:
        return code_list[0]
    else:
        return code_list

def get_code_hash(func):

    if not hasattr(func, '__code__'):
        raise TypeError('parameter passed to get_code_hash must be a function')
    global code_set
    code_set = set() # memoization set
    bytecode_list = _traverse_code(func)
    if isinstance(bytecode_list,list):
        return hash(b''.join(bytecode_list))
    else:
        return hash(bytecode_list)