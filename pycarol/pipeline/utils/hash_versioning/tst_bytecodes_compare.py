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
        print(name)
        print(bytecode)
        print(h)


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
    mp.get_context('spawn')
    q1 = Queue()
    q2 = Queue()
    p1 = Process(target=get_hash_all, args=(function_list,q1,))
    p2 = Process(target=get_hash_all, args=(function_list,q2,))
    p1.start()
    p1.join()
    p2.start()
    p2.join()
    
    l1 = queue_to_list(q1)
    l2 = queue_to_list(q2)
    assert l1==l2
    names1 = [n[0] for n in l1]
    names2 = [n[0] for n in l2]
    hash1 = [n[2] for n in l1]
    hash2 = [n[2] for n in l2]

    for i, name in enumerate(names1):
        print(name)
        print('\t',hash1[i])
        print('\t',hash2[i])
        print('\t',hash1[i]==hash2[i])
