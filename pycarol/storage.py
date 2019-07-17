from .carolina import Carolina


class Storage:
    backend = None
    def __init__(self, carol):
        self.carol = carol
        self._init_if_needed()

    def _init_if_needed(self):
        if Storage.backend is not None:
            return

        carolina = Carolina(self.carol)
        carolina.init_if_needed()
        if carolina.engine == 'GCP-CS':
            from .utils.storage_gcpcs import StorageGCPCS
            Storage.backend = StorageGCPCS(self.carol, carolina)
        elif carolina.engine == 'AWS-S3':
            from .utils.storage_awss3 import StorageAWSS3
            Storage.backend = StorageAWSS3(self.carol, carolina)

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        Storage.backend.save(name, obj, format, parquet, cache)

    def load(self, name, format='pickle', parquet=False, cache=True, storage_space='app_storage', columns=None):
        return Storage.backend.load(name=name, format=format, parquet=parquet, cache=cache, storage_space=storage_space,
                                 columns=columns)

    def files_storage_list(self, app_name=None, all_apps=False, print_paths=False):
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

        return Storage.backend.files_storage_list(app_name=app_name, all_apps=all_apps, print_paths=print_paths)

    def exists(self, name):
        return Storage.backend.exists(name)

    def delete(self, name):
        Storage.backend.delete(name)

    def build_url_parquet_golden(self, dm_name):
        return Storage.backend.build_url_parquet_golden(dm_name)

    def build_url_parquet_staging(self, staging_name, connector_id):
        return Storage.backend.build_url_parquet_staging(staging_name, connector_id)

    def build_url_parquet_staging_master(self, staging_name, connector_id):
        return Storage.backend.build_url_parquet_staging_master(staging_name, connector_id)

    def build_url_parquet_staging_rejected(self, staging_name, connector_id):
        return Storage.backend.build_url_parquet_staging_rejected(staging_name, connector_id)

    def get_dask_options(self):
        return Storage.backend.get_dask_options()

    def get_golden_file_paths(self, dm_name):
        return Storage.backend.get_golden_file_paths(dm_name)

    def get_view_file_paths(self, view_name):
        return Storage.backend.get_view_file_paths(view_name)

    def get_staging_file_paths(self, staging_name, connector_id):
        return Storage.backend.get_staging_file_paths(staging_name, connector_id)
