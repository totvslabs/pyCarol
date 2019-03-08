from multiprocessing import Process
from pycarol.carol_cloner import Cloner
from pycarol.utils.singleton import KeySingleton
from pycarol.carolina import Carolina
from pycarol.storage_gcpcs import StorageGCPCS
from pycarol.storage_awss3 import StorageAWSS3


class Storage(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.storage = None

    def _init_if_needed(self):
        if self.storage is not None:
            return

        carolina = Carolina(self.carol)
        carolina.init_if_needed()
        print(carolina.engine)
        if carolina.engine == 'GCP-CS':
            self.storage = StorageGCPCS(self.carol, carolina)
        elif carolina.engine == 'AWS-S3':
            self.storage = StorageAWSS3(self.carol, carolina)

    def save_async(self, name, obj):
        p = Process(target=_save_async, args=(Cloner(self.carol), name, obj))
        p.start()

        return p

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        self.storage.save(name, obj, format, parquet, cache)

    def load(self, name, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        return self.storage.load(name, format, parquet, cache)

    def exists(self, name):
        self._init_if_needed()
        return self.storage.exists(name)

    def delete(self, name):
        self._init_if_needed()
        self.storage.delete(name)


def _save_async(cloner, name, obj):
    return Storage(cloner.build()).storage.save(name, obj)