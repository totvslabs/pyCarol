from multiprocessing import Process
from collections import defaultdict
from .carol_cloner import Cloner
from .utils.singleton import KeySingleton
from .carolina import Carolina
from .utils.miscellaneous import prettify_path, _attach_path, _FILE_MARKER



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
            from .utils.storage_gcpcs import StorageGCPCS
            self.backend = StorageGCPCS(self.carol, carolina)
        elif carolina.engine == 'AWS-S3':
            from .utils.storage_awss3 import StorageAWSS3
            self.backend = StorageAWSS3(self.carol, carolina)

    def save_async(self, name, obj):
        p = Process(target=_save_async, args=(Cloner(self.carol), name, obj))
        p.start()

        return p

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        self.backend.save(name, obj, format, parquet, cache)

    def load(self, name, format='pickle', parquet=False, cache=True, storage_space='app_storage', columns=None):
        self._init_if_needed()
        return self.backend.load(name=name, format=format, parquet=parquet, cache=cache, storage_space=storage_space,
                                 columns=columns)

    #TODO: put this inside the AWS storage class
    def files_storage_list(self, app_name=None, all_apps=False,  print_paths=False):
        """

        It will return all files in Carol data Storage (CDS).


        :param app_name: `str`, default `None`
            app_name to filter output. If 'None' it will get value used to initialize `Carol()`
        :param all_apps: `bool`, default `False`
            Get all files in CDS. 
        :param print_paths: `bool`, default `False`
            Print the tree structure of the files in CDS
        :return: list of files paths.
        """

        split = f"storage/{self.carol.tenant['mdmId']}/"
        if all_apps:
            prefix = f"storage/{self.carol.tenant['mdmId']}/"

        elif app_name is None:
            app_name = self.carol.app_name
            prefix = f"storage/{self.carol.tenant['mdmId']}/{app_name}/files/"

        else:
            prefix = f"storage/{self.carol.tenant['mdmId']}/{app_name}/files/"

        self._init_if_needed()

        files = list(self.bucket.objects.filter(Prefix=prefix))
        files = [i.key.split(split)[1] for i in files]

        if print_paths:
            main_dict = defaultdict(dict, ((_FILE_MARKER, []),))
            for line in files:
                _attach_path(line, main_dict)
            prettify_path(main_dict)
        return files

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

