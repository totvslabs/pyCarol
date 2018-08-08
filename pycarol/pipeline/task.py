import joblib
import os
import luigi
from luigi import six, parameter
import logging
from pycarol.validator import *


class CarolTarget(luigi.LocalTarget):
    def __init__(self, app):
        self.app = app

    def load(self):
        return self.app.carol.storage.load(self.path)

    def dump(self, function_output):
        self.app.carol.storage.save(function_output, self.path)

    def remove(self):
        self.app.carol.storage.delete(self.path)


class PickleLocalTarget(luigi.LocalTarget):
    def load(self):
        try:
            return joblib.load(self.path)
        except FileNotFoundError:
            print("file not found!!")
            return None

    def dump(self,function_output):
        dir = os.path.dirname(self.path)
        os.makedirs(dir, exist_ok=True)
        joblib.dump(function_output, self.path)

    def remove(self):
        try:
            os.remove(self.path)
            print("file removed")
        except(FileNotFoundError):
            print("file not found")


class KerasLocalTarget(luigi.LocalTarget):
    def load(self):
        from keras.models import load_model
        return load_model(self.path)

    def dump(self,model):
        model.save(self.path)

    def remove(self):
        try:
            os.remove(self.path)
            print("file removed")
        except(FileNotFoundError):
            print("file not found")


class DummyTarget(luigi.LocalTarget):
    def load(self):
        return None

    def dump(self,model):
        pass

    def remove(self):
        pass


mem_data = {}
class MemoryTarget(object):
    def __init__(self, path):
        self.path = path
        print("New target @ " + path)

    def exists(self):
        return self.path in mem_data

    def load(self):
        print(mem_data)
        return mem_data[self.path]

    def dump(self, value):
        print("Saving memory target!! " + str(value) + " to " + str(self.path))
        mem_data[self.path] = value

    def remove(self):
        mem_data.pop(self.path, None)


class Task(luigi.Task):
    TARGET_DIR = './luigi_targets/'
    requires_list = []
    _carol = None

    def buildme(self, local_scheduler=True, **kwargs):
        logging.getLogger('pipeline-interface').setLevel(logging.INFO)
        return luigi.build([self, ], local_scheduler=local_scheduler, **kwargs)

    def carol(self):
        if self._carol is None:
            self._carol = self.carol_builder.build()
        return self._carol

    def build_validator(self):
        return Validator(self.carol())

    def _file_id(self):
        # returns the output default file identifier
        sp = self.to_str_params()
        sp['_carol.tenant'] = str(self.carol().tenant['mdmId'])
        sp['_carol.app_name'] = str(self.carol().app_name)

        return luigi.task.task_id_str(self.get_task_family(), sp)

    def dummy_target(self):
        return DummyTarget(is_tmp=True)

    def carol_target(self):
        s = self._file_id()
        return CarolTarget(s)

    def disk_target(self):
        s = self._file_id()
        path = os.path.join(self.TARGET_DIR,'{}.pkl'.format(s))
        return PickleLocalTarget(path)

    def keras_target(self):
        s = self._file_id()
        path = os.path.join(self.TARGET_DIR,'{}.h5'.format(s))
        return KerasLocalTarget(path)

    def memory_target(self):
        s = self._file_id()
        return MemoryTarget(s)

    def requires(self):
        logging.getLogger('pipeline-interface').setLevel(logging.INFO)

        result_list = []
        for t in self.requires_list:
            fixed_params = {}
            if type(t) is tuple:
                fixed_params = t[1]
                t = t[0]
            task_instance = self.clone(t, **fixed_params)
            task_instance.carol_builder = self.carol_builder
            result_list.append(task_instance)
        return result_list

    def output(self):
        if hasattr(self, 'set_target'):
            return self.set_target()
        #switch here between different target types
        return self.disk_target()

    def load(self):
        return self.output().load()

    def remove(self):
        self.output().remove()

    def run(self):
        logging.getLogger('pipeline-interface').setLevel(logging.INFO)
        #print("run..." + str(self))
        function_inputs = [input_i.load() for input_i in self.input()]
        function_output = self.easy_run(function_inputs)
        self.output().dump(function_output)

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
                raise parameter.UnknownParameterException('%s: takes at most %d parameters (%d given)' % (exc_desc, len(positional_params), len(args)))
            param_name, param_obj = positional_params[i]
            result[param_name] = param_obj.normalize(arg)

        # Then the keyword arguments
        for param_name, arg in six.iteritems(kwargs):
            if param_name in result:
                raise parameter.DuplicateParameterException('%s: parameter %s was already set as a positional parameter' % (exc_desc, param_name))
            if param_name not in params_dict:
                # raise parameter.UnknownParameterException('%s: unknown parameter %s' % (exc_desc, param_name))
                print('%s: unknown parameter %s' % (exc_desc, param_name))
                continue
            result[param_name] = params_dict[param_name].normalize(arg)

        # Then use the defaults for anything not filled in
        for param_name, param_obj in params:
            if param_name not in result:
                if not param_obj.has_task_value(task_family, param_name):
                    raise parameter.MissingParameterException("%s: requires the '%s' parameter to be set" % (exc_desc, param_name))
                result[param_name] = param_obj.task_value(task_family, param_name)

        def list_to_tuple(x):
            """ Make tuples out of lists and sets to allow hashing """
            if isinstance(x, list) or isinstance(x, set):
                return tuple(x)
            else:
                return x
        # Sort it by the correct order and make a list
        return [(param_name, list_to_tuple(result[param_name])) for param_name, param_obj in params]


class inherit_list(object):#class decorator
    def __init__(self, *task_to_inherit_list):
        self.requires_list = list(task_to_inherit_list)

    def __call__(self, task_that_inherits):
        task_that_inherits.requires_list = self.requires_list
        for task_to_inherit in task_that_inherits.requires_list:
            # Get all parameter objects from the underlying task
            fixed_params={}
            if type(task_to_inherit) is tuple:
                fixed_params = task_to_inherit[1]
                task_to_inherit = task_to_inherit[0]

            for param_name, param_obj in task_to_inherit.get_params():
                if param_name in fixed_params: # do not inherit fixed params
                    continue
                # Check if the parameter exists in the inheriting task
                if not hasattr(task_that_inherits, param_name):
                    # If not, add it to the inheriting task
                    setattr(task_that_inherits, param_name, param_obj)

        return task_that_inherits
