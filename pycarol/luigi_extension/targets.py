import luigi
import pandas as pd
import os
import joblib


### Cloud Targets

class PyCarolTarget(luigi.Target):
    """
    This is an abstract cloud target. Not to be called directly.

    In order to use PyCarol Targets, env or files configuration should allow Carol authentication
    with no parameters, Carol().
    If more than one tenant are used in same session, luigi parameter "tenant" should exist.
    """
    login_cache = None
    tenant_cache = None
    storage_cache = None
    def __init__(self, task, *args, **kwargs):
        from pycarol.carol import Carol
        from pycarol.storage import Storage

        if (PyCarolTarget.login_cache and PyCarolTarget.storage_cache) and (PyCarolTarget.tenant_cache == task.tenant):
            self.login = PyCarolTarget.login_cache
            self.storage = PyCarolTarget.storage_cache
        else:
            self.login = Carol()
            self.storage = Storage(self.login)
            PyCarolTarget.login_cache = self.login
            PyCarolTarget.storage_cache = self.storage
            PyCarolTarget.tenant_cache = task.tenant #TODO: make cache more robust, not depending on task.tenant

        namespace = task.get_task_namespace()
        file_id = task._file_id()
        ext = '.' + self.FILE_EXT
        path = os.path.join(namespace, file_id + ext)
        self.path = os.path.join('pipeline', path)



class PicklePyCarolTarget(PyCarolTarget):
    FILE_EXT = 'pkl'

    def load(self):
        return self.storage.load(self.path, format='joblib', cache=False)

    def dump(self, function_output):
        self.storage.save(self.path, function_output, format='joblib', cache=False)

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)

class PytorchPyCarolTarget(PyCarolTarget):
    FILE_EXT = 'pth'

    def load(self):
        import torch
        local_path = self.storage.load(self.path, format='file')
        return torch.load(local_path)

    def dump(self, model_state_dict):
        import torch
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        torch.save(model_state_dict, self.path)
        self.storage.save(self.path, function_output, format='file')

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)

### Local Targets


class LocalTarget(luigi.LocalTarget):
    is_tmp=False
    FILE_EXT = 'ext'
    def __init__(self, task, *args, **kwargs):

        os.makedirs(task.TARGET_DIR, exist_ok=True)
        namespace = task.get_task_namespace()
        file_id = task._file_id()
        ext = '.' + self.FILE_EXT
        path = os.path.join(task.TARGET_DIR, namespace, file_id + ext)
        super().__init__(path=path, *args, **kwargs)


class PickleLocalTarget(LocalTarget):
    FILE_EXT = 'pkl'

    def load(self):
        return joblib.load(self.path)

    def dump(self, function_output):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        joblib.dump(function_output, self.path)

    def remove(self):
        try:
            os.remove(self.path)
        except(FileNotFoundError):
            print("file not found")


class ParquetLocalTarget(LocalTarget):
    FILE_EXT = 'parquet'

    def load(self):
        return pd.read_parquet(self.path)

    def dump(self, function_output):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        function_output.to_parquet(self.path, engine='fastparquet', has_nulls='infer')

    def remove(self):
        try:
            os.remove(self.path)
            print("file removed")
        except(FileNotFoundError):
            print("file not found")


class KerasLocalTarget(LocalTarget):
    FILE_EXT = 'h5'

    def load(self):
        from keras.models import load_model
        return load_model(self.path)

    def dump(self, model):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        model.save(self.path)

    def remove(self):
        try:
            os.remove(self.path)
            print("file removed")
        except(FileNotFoundError):
            print("file not found")


class PytorchLocalTarget(LocalTarget):
    FILE_EXT = 'pth'

    def load(self):
        import torch
        return torch.load(self.path)

    def dump(self, model_state_dict):
        import torch
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        torch.save(model_state_dict, self.path)

    def remove(self):
        try:
            os.remove(self.path)
            print("file removed")
        except(FileNotFoundError):
            print("file not found")


class DummyTarget(LocalTarget):

    def load(self):
        return None

    def dump(self, model):
        pass

    def remove(self):
        pass


class JsonLocalTarget(LocalTarget):
    FILE_EXT = 'json'

    def load(self):
        return pd.read_json(self.path)

    def dump(self, function_output):
        #TODO: json only works for dataframe
        function_output.to_json(self.path)

    def remove(self):
        os.remove(self.path)


class FeatherLocalTarget(LocalTarget):
    FILE_EXT = 'feather'

    def load(self):
        import feather
        return feather.read_dataframe(self.path)

    def dump(self, function_output):
        import feather
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        feather.write_dataframe(function_output, self.path)

    def remove(self):
        os.remove(self.path)
