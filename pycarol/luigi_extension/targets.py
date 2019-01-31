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
    is_cloud_target = True
    def __init__(self, task, *args, **kwargs):
        from ..carol import Carol
        from ..storage import Storage

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
        self.path = os.path.join('pipeline', namespace, "{}.{}".format(file_id,self.FILE_EXT))
        self.log_path = os.path.join('pipeline',namespace, "{}_log.pkl".format(file_id))


    def persistlog(self,string):
        self.storage.save( self.log_path, string,format='joblib')

    def loadlog(self):
        try:
            text = self.storage.load(self.log_path, format='joblib')
        except Exception:
            return str(Exception)
        if not text:
            return "Log not found. log path: {}".format(self.log_path)
        return text

    def removelog(self):
        self.storage.delete(self.log_path)




class PyCarolTempTarget(PyCarolTarget):

    FILE_EXT = 'pkl'

    def load(self):
        return self.storage.load(self.path, format='file', cache=False)

    def dump(self, path):
        self.storage.save(self.path, path, format='file', cache=False)

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)

    def temporary_path(self):
        """
        A context manager that enables a reasonably short, general and
        magic-less way to solve the :ref:`AtomicWrites`.

         * On *entering*, it will create the parent directories so the
           temporary_path is writeable right away.
           This step uses :py:meth:`FileSystem.mkdir`.
         * On *exiting*, it will move the temporary file if there was no exception thrown.
           This step uses :py:meth:`FileSystem.rename_dont_move`

        The file system operations will be carried out by calling them on :py:attr:`fs`.

        The typical use case looks like this:

        .. code:: python

            class MyTask(luigi.Task):
                def output(self):
                    return MyFileSystemTarget(...)

                def run(self):
                    with self.output().temporary_path() as self.temp_output_path:
                        run_some_external_command(output_path=self.temp_output_path)
        """
        class _Manager(object):
            import random
            import errno
            target = self

            def __init__(self):
                num = random.randrange(0, 1e10)
                slashless_path = self.target.path.rstrip('/').rstrip("\\")
                self._temp_path = '{}-luigi-tmp-{:010}{}'.format(
                    slashless_path,
                    num,
                    self.target._trailing_slash())
                # TODO: os.path doesn't make sense here as it's os-dependent
                tmp_dir = os.path.dirname(slashless_path)
                if tmp_dir:
                    import os
                    os.makedirs(tmp_dir, exist_ok=True)

            def __enter__(self):
                return self._temp_path

            def __exit__(self, exc_type, exc_value, traceback):
                if exc_type is None:
                    # There were no exceptions
                    self.target.dump(self._temp_path)
                return False  # False means we don't suppress the exception

        return _Manager()

    def _trailing_slash(self):
        # I suppose one day schema-like paths, like
        # file:///path/blah.txt?params=etc can be parsed too
        return self.path[-1] if self.path[-1] in r'\/' else ''


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
        self.storage.save(self.path, self.path, format='file')

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)

class KerasPyCarolTarget(PyCarolTarget):
    FILE_EXT = 'h5'

    def load(self):
        from keras.models import load_model
        local_path = self.storage.load(self.path, format='file')
        return load_model(local_path)

    def dump(self, model):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        model.save(self.path)
        self.storage.save(self.path, self.path, format='file')

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)


### Local Targets


class LocalTarget(luigi.LocalTarget):
    is_tmp=False
    FILE_EXT = 'ext'
    is_cloud_target = False
    def __init__(self, task, *args, **kwargs):

        os.makedirs(task.TARGET_DIR, exist_ok=True)
        namespace = task.get_task_namespace()
        file_id = task._file_id()
        ext = '.' + self.FILE_EXT
        path = os.path.join(task.TARGET_DIR, namespace, file_id + ext)
        super().__init__(path=path, *args, **kwargs)

    def loadlog(self):
        return "task log not implemented for local targets"

    def removelog(self):
        return "task log not implemented for local targets"

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

    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))

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
