import luigi
import pandas as pd
import os
import joblib
from deprecated import deprecated


### Cloud Targets
class LocalTarget(luigi.LocalTarget):
    FILE_EXT = ''
    is_tmp = False

    def __init__(self, task, path=None, *args, **kwargs):
        os.makedirs(task.TARGET_DIR, exist_ok=True)
        namespace = task.get_task_namespace()
        if path is None:
            file_id = task._file_id()
            ext = '.' + self.FILE_EXT
            file_id = file_id.split(namespace+'.')[-1]  #this will prevent to copy all the module path to the name of the file.
            path = os.path.join(task.TARGET_DIR, namespace, file_id + ext)
        super().__init__(path=path, *args, **kwargs)

    def loadlog(self):
        return "task log not implemented for local targets"

    def removelog(self):
        return "task log not implemented for local targets"


class CDSTarget(LocalTarget):
    """ A target that works both locally and on Carol Data Storage, based on env parameter CLOUD_TARGET

    In order to use Carol Data Storage Targets, env or files configuration should allow Carol authentication
    with no parameters, Carol().
    If more than one tenant are used in same session, luigi parameter "tenant" should exist.

    """
    login_cache = None
    tenant_cache = None
    storage_cache = None

    def __init__(self, task, *args, **kwargs):
        super().__init__(task, *args, **kwargs)

        self._is_cloud_target = task.is_cloud_target if task.is_cloud_target is not None else \
            os.environ.get('CLOUD_TARGET', 'false').lower() == 'true'
        
        if self._is_cloud_target:
            from ..carol import Carol
            from ..storage import Storage

            # We CANNOT cache the storage with GCP because the GCP API is not thread safe and would result in SSL errors
            # if luigi is using more than 1 worker
            if (CDSTarget.login_cache) and (CDSTarget.tenant_cache == task.tenant):
                self.login = CDSTarget.login_cache
                # self.storage = PyCarolTarget.storage_cache
            else:
                self.login = Carol()
                # self.storage = Storage(self.login)
                CDSTarget.login_cache = self.login
                # PyCarolTarget.storage_cache = self.storage
                CDSTarget.tenant_cache = task.tenant #TODO: make cache more robust, not depending on task.tenant
    
            # Storage var needs to be always created per Target
            self.storage = Storage(self.login)
    
            namespace = task.get_task_namespace()
            file_id = task._file_id()
            file_id = file_id.split(namespace+'.')[-1] #this will prevent to copy all the module path to the name of the file.
            self._local_path = self.path    # Save a copy of the local path, before modifying it: This is useful when
                                            # the target needs to use a local path
            self.path = os.path.join('pipeline', namespace, "{}.{}".format(file_id, self.FILE_EXT))
            self.log_path = os.path.join('pipeline',namespace, "{}_log.pkl".format(file_id))

    def persistlog(self, string):
        self.storage.save(self.log_path, string,format='joblib')

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

    def load(self, *args, **kwargs):
        if self._is_cloud_target:
            return self.load_cds(*args, **kwargs)
        return self.load_local(*args, **kwargs)

    def dump(self, *args, **kwargs):
        if self._is_cloud_target:
            return self.dump_cds(*args, **kwargs)
        return self.dump_local(*args, **kwargs)

    def remove(self, *args, **kwargs):
        if self._is_cloud_target:
            return self.remove_cds(*args, **kwargs)
        return self.remove_local(*args, **kwargs)

    def exists(self, *args, **kwargs):
        if self._is_cloud_target:
            return self.exists_cds(*args, **kwargs)
        return self.exists_local(*args, **kwargs)

    def load_local(self, *args, **kwargs):
        return super().load(*args, **kwargs)

    def dump_local(self, *args, **kwargs):
        return super().dump(*args, **kwargs)

    def exists_local(self, *args, **kwargs):
        return super().exists(*args, **kwargs)

    def remove_local(self, *args, **kwargs):
        return super().remove(*args, **kwargs)

    def remove_cds(self):
        self.storage.delete(self.path)

    def exists_cds(self):
        return self.storage.exists(self.path)
    

class FileTarget(CDSTarget):
    """
    This target operates with filepaths.
    easy_run should return a filepath for a local temporary file. This file will be removed after been sent to Carol.
    When loading this target, the file is copied from Carol to a local file. On easy_run we receive the local filepath.
    Important note: when loading the target, its local copy will not be automatically removed.
    """

    FILE_EXT = 'file'

    # TODO NOW: Define how it should be done locally

    def load_cds(self):
        return self.storage.load(self.path, format='file', cache=False)

    def dump_cds(self, tempfile_path):
        self.storage.save(self.path, tempfile_path, format='file', cache=False)
        os.remove(tempfile_path)


class PickleTarget(CDSTarget):
    FILE_EXT = 'pkl'

    def load_cds(self):
        return self.storage.load(self.path, format='joblib', cache=False)

    def dump_cds(self, function_output):
        self.storage.save(self.path, function_output, format='joblib', cache=False)

    def load_local(self):
        return joblib.load(self.path)

    def dump_local(self, function_output):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        joblib.dump(function_output, self.path)


class ParquetTarget(CDSTarget):
    FILE_EXT = 'parquet'

    def load_cds(self, **kwargs):
        return self.storage.load(self.path, format='joblib', cache=True, parquet=True, **kwargs)

    def dump_cds(self, function_output):
        self.storage.save(self.path, function_output, format='joblib', cache=False, parquet=True)

    def load_local(self, **kwargs):
        return pd.read_parquet(self.path, **kwargs)

    def dump_local(self, function_output):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        function_output.to_parquet(self.path, engine='fastparquet', has_nulls='infer')


class KerasTarget(CDSTarget):
    FILE_EXT = 'h5'

    def load_cds(self):
        from keras.models import load_model
        local_path = self.storage.load(self.path, format='file')
        return load_model(local_path)

    def dump_cds(self, model):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        model.save(self.path)
        self.storage.save(self.path, self.path, format='file')

    def load_local(self):
        from keras.models import load_model
        return load_model(self.path)

    def dump_local(self, model):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        model.save(self.path)


class PytorchTarget(CDSTarget):
    FILE_EXT = 'pth'

    def load_cds(self):
        import torch
        local_path = self.storage.load(self.path, format='file')
        return torch.load(local_path)

    def dump_cds(self, model_state_dict):
        import torch
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        torch.save(model_state_dict, self.path)
        self.storage.save(self.path, self.path, format='file')

    def load_local(self):
        import torch
        return torch.load(self.path)

    def dump_local(self, model_state_dict):
        import torch
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        torch.save(model_state_dict, self.path)


class DummyTarget:

    def __init__(self, fixed_output=None, *args, **kwargs):
        self.fixed_output = fixed_output

    def exists(self):
        return True

    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))

    def load(self):
        return self.fixed_output

    def dump(self, model):
        pass

    def remove(self):
        pass


class JsonTarget(CDSTarget):
    FILE_EXT = 'json'

    # TODO NOW: Define how to do that on the CDS

    def load_local(self):
        return pd.read_json(self.path)

    def dump_local(self, function_output):
        #TODO: json only works for dataframe
        function_output.to_json(self.path)


class FeatherTarget(CDSTarget):
    FILE_EXT = 'feather'

    # TODO NOW: Define how to do that on the CDS

    def load_local(self):
        import feather
        return feather.read_dataframe(self.path)

    def dump_local(self, function_output):
        import feather
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        feather.write_dataframe(function_output, self.path)

# ########### --------------------------- DEPRECATED ---------------------------   ############## #

@deprecated
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

        # We CANNOT cache the storage with GCP because the GCP API is not thread safe and would result in SSL errors
        # if luigi is using more than 1 worker
        if (PyCarolTarget.login_cache) and (PyCarolTarget.tenant_cache == task.tenant):
            self.login = PyCarolTarget.login_cache
            # self.storage = PyCarolTarget.storage_cache
        else:
            self.login = Carol()
            # self.storage = Storage(self.login)
            PyCarolTarget.login_cache = self.login
            # PyCarolTarget.storage_cache = self.storage
            PyCarolTarget.tenant_cache = task.tenant #TODO: make cache more robust, not depending on task.tenant

        # Storage var needs to be always created per Target
        self.storage = Storage(self.login)

        namespace = task.get_task_namespace()
        file_id = task._file_id()
        file_id = file_id.split(namespace+'.')[-1] #this will prevent to copy all the module path to the name of the file.
        self.path = os.path.join('pipeline', namespace, "{}.{}".format(file_id, self.FILE_EXT))
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


@deprecated
class PyCarolFileTarget(PyCarolTarget):
    """
    This target operates with filepaths.
    easy_run should return a filepath for a local temporary file. This file will be removed after been sent to Carol.
    When loading this target, the file is copied from Carol to a local file. On easy_run we receive the local filepath.
    Important note: when loading the target, its local copy will not be automatically removed.
    """

    FILE_EXT = 'file'

    def load(self):
        return self.storage.load(self.path, format='file', cache=False)

    def dump(self, tempfile_path):
        self.storage.save(self.path, tempfile_path, format='file', cache=False)
        os.remove(tempfile_path)

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)


@deprecated
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


@deprecated
class ParquetPyCarolTarget(PyCarolTarget):
    FILE_EXT = 'parquet'

    def load(self, **kwargs):
        return self.storage.load(self.path, format='joblib', cache=True, parquet=True, **kwargs)

    def dump(self, function_output):
        self.storage.save(self.path, function_output, format='joblib', cache=False, parquet=True)

    def remove(self):
        self.storage.delete(self.path)

    def exists(self):
        return self.storage.exists(self.path)


@deprecated
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


@deprecated
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

@deprecated
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

@deprecated
class ParquetLocalTarget(LocalTarget):
    FILE_EXT = 'parquet'

    def load(self, **kwargs):
        return pd.read_parquet(self.path, **kwargs)

    def dump(self, function_output):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        function_output.to_parquet(self.path, engine='fastparquet', has_nulls='infer')

    def remove(self):
        try:
            os.remove(self.path)
            print("file removed")
        except(FileNotFoundError):
            print("file not found")

@deprecated
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

@deprecated
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

@deprecated
class DummyTarget:

    def __init__(self, fixed_output=None, *args, **kwargs):
        self.fixed_output = fixed_output

    def exists(self):
        return True

    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))

    def load(self):
        return self.fixed_output

    def dump(self, model):
        pass

    def remove(self):
        pass

@deprecated
class JsonLocalTarget(LocalTarget):
    FILE_EXT = 'json'

    def load(self):
        return pd.read_json(self.path)

    def dump(self, function_output):
        #TODO: json only works for dataframe
        function_output.to_json(self.path)

    def remove(self):
        os.remove(self.path)

@deprecated
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

