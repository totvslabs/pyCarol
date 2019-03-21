from multiprocessing import Process
from pycarol.carol_cloner import Cloner
from pycarol.utils.singleton import KeySingleton
from pycarol.carolina import Carolina
from pycarol.storage_gcpcs import StorageGCPCS
from pycarol.storage_awss3 import StorageAWSS3


class Storage(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.backend = None

    def _init_if_needed(self):
        if self.backend is not None:
            return

        carolina = Carolina(self.carol)
        carolina.init_if_needed()
        if carolina.engine == 'GCP-CS':
            self.backend = StorageGCPCS(self.carol, carolina)
        elif carolina.engine == 'AWS-S3':
            self.backend = StorageAWSS3(self.carol, carolina)

    def save_async(self, name, obj):
        p = Process(target=_save_async, args=(Cloner(self.carol), name, obj))
        p.start()

        return p

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        self.backend.save(name, obj, format, parquet, cache)

    def load(self, name, format='pickle', parquet=False, cache=True, storage_space='app_storage'):
        self._init_if_needed()
        return self.backend.load(name, format, parquet, cache, storage_space)

    def exists(self, name):
        self._init_if_needed()
        return self.backend.exists(name)

    def delete(self, name):
        self._init_if_needed()
        self.backend.delete(name)

    def build_url_parquet_golden(self, dm_name):
        self._init_if_needed()
        return self.backend.build_url_parquet_golden(dm_name)

    def build_url_parquet_staging(self, staging_name, connector_id):
        self._init_if_needed()
        return self.backend.build_url_parquet_staging(staging_name, connector_id)

    def build_url_parquet_staging_master(self, staging_name, connector_id):
        self._init_if_needed()
        return self.backend.build_url_parquet_staging_master(staging_name, connector_id)

    def build_url_parquet_staging_rejected(self, staging_name, connector_id):
        self._init_if_needed()
        return self.backend.build_url_parquet_staging_rejected(staging_name, connector_id)

    def get_dask_options(self):
        self._init_if_needed()
        return self.backend.get_dask_options()

    def get_golden_file_paths(self, dm_name):
        self._init_if_needed()
        return self.backend.get_golden_file_paths(dm_name)

    def get_staging_file_paths(self, staging_name, connector_id):
        self._init_if_needed()
        return self.backend.get_staging_file_paths(staging_name, connector_id)


def _save_async(cloner, name, obj):
    return Storage(cloner.build()).backend.save(name, obj)
