from .task import *

def f1(x):
    return x+1

def f2(x):
    return x+1

def f3(x):
    return f3(x+1)

def test_empty_task():
    class Task1(Task):
        pass
    t1 = Task1()
    assert t1

def test_if_task_generates_hash():
    class Task1(Task):
        task_function = f1
    t1 = Task1()
    h = t1.hash_version()
    assert isinstance(h,int)
    assert h !=0


def test_if_tasks_has_equal_hash():
    class Task1(Task):
        task_function = f1

    class Task2(Task):
        task_function = f2

    assert Task1().hash_version() == Task2().hash_version()

def test_if_tasks_has_different_hash_1():
    class Task1(Task):
        task_function = f1
        
    class Task3(Task):
        task_function = f3

    assert Task1().hash_version() != Task3().hash_version()

def test_if_tasks_has_different_hash_2():
    class Task1(Task):
        pass
        
    class Task2(Task):
        task_function = f1

    assert Task1().hash_version() != Task2().hash_version()

