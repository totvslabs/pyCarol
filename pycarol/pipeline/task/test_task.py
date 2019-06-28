from .task import *

def test_empty_task():
    class Task1(Task):
        pass
    t1 = Task1()
    assert t1

def test_if_task_generates_hash():
    class Task1(Task):
        pass
    t1 = Task1()
    h = t1.hash_version()
    print(f"hash_version={h}")
    assert h


def test_if_tasks_has_equal_hash():
    class Task1(Task):
        pass

    class Task2(Task):
        pass

    assert Task1().hash_version() == Task2().hash_version()

def test_if_tasks_has_different_hash_1():
    class Task1(Task):
        pass
        
    class Task2(Task):
        def run(self,inputs):
            return None

    assert Task1().hash_version() != Task2().hash_version()

def test_if_tasks_has_different_hash_2():
    class Task1(Task):
        def easy_run(self,inputs):
            return inputs
        
    class Task2(Task):
        def easy_run(self,inputs):
            return None

    assert Task1().hash_version() != Task2().hash_version()
