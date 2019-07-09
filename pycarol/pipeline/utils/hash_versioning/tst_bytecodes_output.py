from pycarol.pipeline.utils.hash_versioning.test_hash_versioning import (
    equal_functions_list, different_functions_list
)

function_set = set()
for a,b in equal_functions_list + different_functions_list:
    function_set.add(a)
    function_set.add(b)

function_list = [f for f in function_set]

from pycarol.pipeline.utils.hash_versioning import get_bytecode_tree, get_function_hash
def get_hash(f):
    name = f.__name__

    try:
        bytecode = get_bytecode_tree(f)
    except:
        bytecode = "FAIL"
    try:
        h = get_function_hash(f)
    except:
        h = "FAIL"
    
    return([name, bytecode, h])

def get_hash_all(f_list,q):
    for f in f_list:
        q.put(get_hash(f))

def print_all():
    for f in function_set:
        name, bytecode, h = get_hash(f)
        print(name,bytecode)
        # print('\t',h)


import multiprocessing as mp

from multiprocessing import Process, Queue
import time
import os


def queue_to_list(q)->list:
    l=[]
    while not q.empty():
        l.append(q.get())
    return l

if __name__ == '__main__':
    print_all()