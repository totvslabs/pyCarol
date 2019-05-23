import luigi
from luigi import parameter, six
from luigi.task import flatten
from .visualization import Visualization
from .targets import PickleLocalTarget, DummyTarget, PytorchLocalTarget, KerasLocalTarget
import logging
import warnings

luigi.build([], workers=1, local_scheduler=True)

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)


class Task(luigi.Task):
    TARGET_DIR = './luigi_targets/'  # this class attribute can be redefined somewhere else

    TARGET = PickleLocalTarget  # DEPRECATED!
    target_type = PickleLocalTarget
    visualization_class = Visualization

    persist_stdout = False
    requires_list = []
    requires_dict = {}
    resources = {'cpu': 1}  # default resource to be overridden or complemented


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.visualize = self.visualization_class(task=self)

    def buildme(self, local_scheduler=True, **kwargs):
        luigi.build([self, ], local_scheduler=local_scheduler, **kwargs)

    def debug_task(self):
        persist_stdout =self.persist_stdout
        self.persist_stdout = False
        self.run()
        self.persist_stdout = persist_stdout

    def _file_id(self):
        # returns the output default file identifier
        return luigi.task.task_id_str(self.get_task_family(), self.to_str_params(only_significant=True))

    def _txt_path(self):
        return "{}.txt".format(self._file_id())

    def requires(self):
        if len(self.requires_list) > 0:
            result_list = []
            for t in self.requires_list:
                fixed_params = {}
                if type(t) is tuple:
                    fixed_params = t[1]
                    t = t[0]
                task_instance = self.clone(t, **fixed_params)
                result_list.append(task_instance)
            return result_list
        elif len(self.requires_dict) > 0:
            result_dict = {}
            for k, t in self.requires_dict.items():
                fixed_params = {}
                if type(t) is tuple:
                    fixed_params = t[1]
                    t = t[0]
                task_instance = self.clone(t, **fixed_params)
                result_dict.update({k: task_instance})
            return result_dict
        else:
            return []

    def output(self):
        if self.TARGET != PickleLocalTarget:  # Check for deprecated use
            warnings.warn('TARGET is being replaced with target_type.', DeprecationWarning)
            return self.TARGET(self)

        return self.target_type(self)

    def load(self, **kwargs):
        return self.output().load(**kwargs)

    def remove(self):
        self.output().remove()
        self.output().removelog()

    def loadlog(self):
        return self.output().loadlog()

    def run(self):

        if isinstance(self.input(), list):
            function_inputs = [input_i.load(**self.load_input_params(input_i)) if self.load_input_params(input_i)
                               else input_i.load() for input_i in self.input()]
        elif isinstance(self.input(), dict):
            function_inputs = {i: (input_i.load(**self.load_input_params(input_i))
                                   if self.load_input_params(input_i) else input_i.load())
                               for i, input_i in self.input().items()}

        if self.output().is_cloud_target and self.persist_stdout:
            from contextlib import redirect_stdout
            try:
                import io
                f = io.StringIO()
                with redirect_stdout(f):
                    function_output = self._easy_run(function_inputs)
            finally:
                # self.output().persistlog(self._txt_path())
                self.output().persistlog(f.getvalue())
        else:
            function_output = self._easy_run(function_inputs)
        print("dumping task",self)

        self.output().dump(function_output)


    def _easy_run(self, inputs):
        # Override this method to implement standard pre/post-processing
        return self.easy_run(inputs)

    def easy_run(self, inputs):
        return None

    #this method was changed from the original version to allow execution of a task
    #with extra parameters. the original one, raises an exception. now, we print that exception
    #in this version we do not raise neither print it.
    @classmethod
    def get_param_values(cls, params, args, kwargs):
        """
        Get the values of the parameters from the args and kwargs.
        :param params: list of (param_name, Parameter).
        :param args: positional arguments
        :param kwargs: keyword arguments.
        :returns: list of `(name, value)` tuples, one for each parameter.
        """
        result = {}

        params_dict = dict(params)

        task_family = cls.get_task_family()

        # In case any exceptions are thrown, create a helpful description of how the Task was invoked
        # TODO: should we detect non-reprable arguments? These will lead to mysterious errors
        exc_desc = '%s[args=%s, kwargs=%s]' % (task_family, args, kwargs)

        # Fill in the positional arguments
        positional_params = [(n, p) for n, p in params if p.positional]
        for i, arg in enumerate(args):
            if i >= len(positional_params):
                raise parameter.UnknownParameterException(
                    '%s: takes at most %d parameters (%d given)' % (exc_desc, len(positional_params), len(args)))
            param_name, param_obj = positional_params[i]
            result[param_name] = param_obj.normalize(arg)

        # Then the keyword arguments
        for param_name, arg in six.iteritems(kwargs):
            if param_name in result:
                raise parameter.DuplicateParameterException(
                    '%s: parameter %s was already set as a positional parameter' % (exc_desc, param_name))
            if param_name not in params_dict:
                # raise parameter.UnknownParameterException('%s: unknown parameter %s' % (exc_desc, param_name))
                continue

            result[param_name] = params_dict[param_name].normalize(arg)

        # Then use the defaults for anything not filled in
        for param_name, param_obj in params:
            if param_name not in result:
                if not param_obj.has_task_value(task_family, param_name):
                    raise parameter.MissingParameterException(
                        "%s: requires the '%s' parameter to be set" % (exc_desc, param_name))
                result[param_name] = param_obj.task_value(task_family, param_name)

        def list_to_tuple(x):
            """ Make tuples out of lists and sets to allow hashing """
            if isinstance(x, list) or isinstance(x, set):
                return tuple(x)
            else:
                return x

        # Sort it by the correct order and make a list
        return [(param_name, list_to_tuple(result[param_name])) for param_name, param_obj in params]

    def get_execution_params(self):
        params = {}
        for param_name in dir(self.__class__):
            param_obj = getattr(self, param_name)
            params.update({param_name: param_obj})

        return params

    def load_input_params(self, input_target):
        """
        Overwrite this if need to pass parameters when loading a requirement.

        :param input_target:
            Target that will be loaded.
        :return: `Dict`
            Return a dict with key/value parameters to be passed to Target.load()
        """
        return {}



class WrapperTask(Task):
    """
    Use for tasks that only wrap other tasks and that by definition are done if all their requirements exist.
    """
    def run(self):
        pass

    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))

    def output(self):
        return self.input()


def set_attributes(task_to_inherit, task_that_inherits):
    fixed_params = {}

    if type(task_to_inherit) is tuple:
        fixed_params = task_to_inherit[1]
        task_to_inherit = task_to_inherit[0]

    for param_name, param_obj in task_to_inherit.get_params():
        if param_name in fixed_params:  # do not inherit fixed params
            continue

        # Check if the parameter exists in the inheriting task
        if not hasattr(task_that_inherits, param_name):
            # If not, add it to the inheriting task
            setattr(task_that_inherits, param_name, param_obj)
    return task_that_inherits


class inherit_list(object):
    # http://blog.thedigitalcatonline.com/blog/2015/04/23/python-decorators-metaprogramming-with-style/

    def __init__(self, *task_to_inherit_list):
        self.requires_list = list(task_to_inherit_list)

    def __call__(self, task_that_inherits):
        task_that_inherits.requires_list = self.requires_list
        for task_to_inherit in task_that_inherits.requires_list:
            # Get all parameter objects from the underlying task
            task_that_inherits = set_attributes(task_to_inherit, task_that_inherits)

        return task_that_inherits


class inherit_dict(object):
    def __init__(self, **task_to_inherit_dict):
        self.requires_dict = task_to_inherit_dict

    def __call__(self, task_that_inherits):
        task_that_inherits.requires_dict = self.requires_dict

        for key, task_to_inherit in task_that_inherits.requires_dict.items():
            # Get all parameter objects from the underlying task
            task_that_inherits = set_attributes(task_to_inherit, task_that_inherits)
        return task_that_inherits
