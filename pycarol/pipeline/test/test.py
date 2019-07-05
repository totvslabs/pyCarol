import luigi
import os
from ..targets import DummyTarget, PickleTarget
from collections import namedtuple
from unittest.mock import patch, MagicMock, PropertyMock
from contextlib import ExitStack
import shutil
import logging
from luigi.execution_summary import LuigiStatusCode
from pycarol.pipeline import Task
logger = logging.getLogger(__name__)

""" 
# Task Execution
    When testing a Task execution, there a couple of things that could be tested:
        - Execution success
        - Requires
        - Execution
# Mocks
# Special Words when defining Test Cases:
    * Test
    Every test case must start with the 'Test' keyword as default from python.unittest.
    * Flow
    If your test case involves Luigi Extension related tasks, use 'Flow' keyword. E.g. TestFlowMyTest. This way, pipeline 
    extension updates will be able to be checked.
"""


def task_execution_debug(task, parameters=None, worker_scheduler_factory=None, **env_params):
    """ Execute a pipeline pipeline
    :param task:
    :param parameters: dict
    :return: instance of class TaskOutput:
            success: bool,
            worker: worker object,
            task: instance task
            task_history: pipeline's worker _add_task_history
            history_has(task, status, ignore_parameters=False): whether task history has or not that task and status.
                Obs. If you want to ignore parameter, make sure task is a class and not an instance.
    """
    if parameters is None:
        parameters = {}

    if "no_lock" not in env_params:
        env_params["no_lock"] = True

    if "local_scheduler" not in env_params:
        env_params["local_scheduler"] = True

    out = dict()
    # TODO Get only parameters that are used in task_instance. Similar to self.clone
    task_instance = task(**parameters)
    out['task'] = task_instance
    exec_out = luigi.interface._schedule_and_run([task_instance], worker_scheduler_factory,
                                                 override_defaults=env_params)
    # TODO: Check luigi version
    # if luigi.__version__
    out.update({'success': exec_out.status==LuigiStatusCode.SUCCESS})
    task_history = exec_out.worker._add_task_history
    out.update({'task_history': task_history})

    def history_has(task, status, ignore_parameters=True):
        if not ignore_parameters:
            for t, s, _ in task_history:
                if task == t and status == s:
                    return True
            return False
        else:
            for t, s, _ in task_history:
                if task.__name__ == t.__class__.__name__ and status == s:
                    return True
            return False

    out.update({'history_has': history_has})

    # TODO Get execution stacktrace
    return namedtuple("TaskOutput", out.keys())(*out.values())


def pipeline_test(cls):
    """ Mock pipeline Task to have TARGET_DIR inside test directory and erase target files before each test
    """
    new_target = f'luigi_targets/test/{cls.__module__}/{cls.__name__}'
    class_setUp = cls.setUp

    def mocked_setUp(self):
        patcher = patch('pycarol.pipeline.Task.TARGET_DIR', new_callable=PropertyMock, return_value=new_target)
        self.addCleanup(patcher.stop)
        self.mock_target = patcher.start()
        if os.path.isdir(new_target):
            shutil.rmtree(new_target)
        return class_setUp(self)

    cls.setUp = mocked_setUp

    return cls


class mock_task:
    """ Define a task as executed and default return from a specific Task
    This mock will work for all Tasks. If the user wants to mock diferently with different parameters, must specify
    task_parameters.

    Dict Parameters:
        mock_task
        task_output or target_filename
        limit_size:
        target_path:
    """

    def __init__(self, *mock_tasks):
        """
        :param mock_tasks: list of tasks
        """
        self.mock_tasks = mock_tasks

    def __call__(self, exec_func):
        mock_tasks = self.mock_tasks

        def patched_func(self, *args, **kwargs):
            with ExitStack() as stack:
                patches = []  # TODO Initialize only if it does not exist. Necessary for wrapper of decorators
                args = [arg for arg in args if not isinstance(arg, MagicMock) and not isinstance(arg, PropertyMock)]
                for dic in mock_tasks:
                    task = dic['mock_task']
                    if 'task_output' in dic:
                        task_output = dic['task_output']
                    elif 'target_filename' in dic:
                        target_filename = dic['target_filename']
                    else:
                        raise ValueError('Mocked Task must have a predefined task_output or target_filename')

                    if 'limit_size' in dic:
                        task_output = task_output[0:dic['limit_size']]

                    if 'task_parameters' in dic:
                        # TODO handle cases of user having same task with different parameters
                        pass
                    else:
                        if 'task_output' in dic:
                            out_target = DummyTarget(fixed_output=task_output)

                        if 'target_filename' in dic:
                            if 'target_class' in dic:
                                TARGET = dic['target_class']
                            else:
                                TARGET = PickleTarget
                            out_target = TARGET(task, path=target_filename, is_tmp=True)
                            out_target.remove = lambda: None  # Use this to avoid having the file removed

                        if 'target_params' in dic:
                            for param_name, param in dic['target_params'].items():
                                setattr(out_target, param_name, param)

                        patches.append([
                            stack.enter_context(
                                patch.object(task, 'output', return_value=out_target)),
                            stack.enter_context(
                                patch.object(task, 'complete', return_value=True))])
                exec_func(self, *args, **kwargs)

        return patched_func


class mock_task_wrapper:
    """ Define a task as executed and default return from a specific Task, but still executes task's requires
    This mock will work for all Tasks. If the user wants to mock diferently with different parameters, must specify
    task_parameters.
    -- Possible improvements:
    An easier way to define a task as completed or not, without using mocks, would be to get the task's output name
    and create that output using 'task_output' as a pickle.
    """

    def __init__(self, *mock_tasks):
        """
        :param mock_tasks: list of tasks
        """
        self.mock_tasks = mock_tasks

    def __call__(self, exec_func):
        def patched_func(*args, **kwargs):
            with ExitStack() as stack:
                patches = []
                for dic in self.mock_tasks:
                    task = dic['mock_task']
                    if 'task_parameters' in dic:
                        # TODO handle cases of user having same task with different parameters
                        pass
                    if 'task_output' in dic:
                        task_output = dic['task_output']
                        out_target = DummyTarget(is_tmp=True)

                        def new_load():
                            return task_output

                        out_target.load = new_load
                        patches.append(stack.enter_context(
                            patch.object(task, 'output', return_value=out_target)))
                    patches.append(
                        stack.enter_context(
                            patch.object(task, 'complete', side_effect=luigi.WrapperTask.complete)))
                exec_func(*args, **kwargs)

        return patched_func


class TaskA(Task):
    def easy_run(self, inputs):
        return True


def test_task_execution_debug():
    out = task_execution_debug(TaskA)
    assert out.success
    assert out.history_has(TaskA, 'DONE')