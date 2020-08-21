from .carolina import Carolina
from datetime import datetime, timedelta

class Storage:

    """
    Handle all Carol storage interactions.


    Args:
            carol: `class: pycarol.Carol`

    """

    def __init__(self, carol):
        """

        Args:

            carol: `class: pycraol.Carol`
        """

        self.carol = carol
        self._init_if_needed()
        self.carolina = None

    def _init_if_needed(self):
        # TODO: This if seems to be useless. It can be handled in `carolina.init_if_needed()`
        if (Carolina.token is not None) and (Carolina.token.get('tenant_name', '') == self.carol.tenant['mdmName']) and \
           (Carolina.token.get('app_name', '') == self.carol.app_name) and \
           (datetime.utcnow() + timedelta(minutes=1) < datetime.fromtimestamp(Carolina.token.get('expirationTimestamp', 1)/1000.0)):
            pass
            #return
        else:
            Carolina.token = None

        self.carolina = Carolina(self.carol)
        self.carolina.init_if_needed()
        if self.carolina.engine == 'GCP-CS':
            from .utils.storage_gcpcs import StorageGCPCS
            self.backend = StorageGCPCS(self.carol, self.carolina)
        else:
            raise NotImplemented(f"Only 'GCP-CS' backend implemented in this version. "
                                 f"You are trying to use {self.carolina.engine }")

    def save(self, name, obj, format='pickle', parquet=False, cache=True, chunk_size=None):
        """

        Args:

            name: `str`.
                Filename to be used when saving the `obj`
            obj: `obj`
                It depends on the `format` parameter.
            format: `str`
                Possible values:

                    1. `pickle`: It uses `pickle.dump` to save the binary file.
                    2. `joblib`: It uses `joblib.dump` to save the binary file.
                    3. `file`: It saves a local file sending it directly to Carol.

            parquet: `bool` default `False`
                It uses `pandas.DataFrame.to_parquet` to save. `obj` should be a pandas DataFrame
            cache: `bool` default `True`
                Cache the file saved in the temp directory.
            chunk_size: `int` default `None`
                The size of a chunk of data whenever iterating (in bytes).
                This must be a multiple of 256 KB per the API specification.

        Usage:

        Saving a local file in CDS.

        .. code:: python

            from pycarol import Carol, Storage
            import pandas as pd
            login = Carol()
            stg = Storage(login)

            stg.save(name='myfile.csv', obj='/local/file/.csv',  format='file')
            # to load the file use:
            path = stg.load(name='teste.zip',  format='file')
            pd.read_csv(path)

        Saving an object.

        .. code:: python

            my_dict = {"a":1, "b":2}

            from pycarol import Carol, Storage
            login = Carol()
            stg = Storage(login)

            stg.save(name='myfile.json', obj=my_dict,  format='pickle')
            # to load the file use:
            my_dict = stg.load(name='myfile.json',  format='pickle')

        It works for `format=joblib` as well,

        .. code:: python

            my_dict = {"a":1, "b":2}

            from pycarol import Carol, Storage
            login = Carol()
            stg = Storage(login)

            stg.save(name='myfile.json', obj=my_dict,  format='joblib')
            # to load the file use:
            my_dict = stg.load(name='myfile.json',  format='joblib')

        Saving a pandas DataFrame

        .. code:: python

            import pandas as pd
            from pycarol import Carol, Storage

            d = {'col1': [1, 2], 'col2': [3, 4]}
            df = pd.DataFrame(data=d)

            login = Carol()
            stg = Storage(login)

            stg.save(name='myfile.parquet', obj=my_dict,  parquet=True)
            # to load the file use:
            df = stg.load(name='myfile.parquet', parquet=True)


        """
        self.backend.save(name, obj, format, parquet, cache, chunk_size)

    def load(self, name, format='pickle', parquet=False, cache=True, storage_space='app_storage', columns=None,
             chunk_size=None):
        """

        Args:

            name: `str`.
                Filename to be load
            format: `str`
                Possible values:

                    1. `pickle`: It uses `pickle.dump` to save the binary file.
                    2. `joblib`: It uses `joblib.dump` to save the binary file.
                    3. `file`: It saves a local file sending it directly to Carol.

            parquet: `bool` default `False`
                It uses `pandas.DataFrame.to_parquet` to save. `obj` should be a pandas DataFrame
            cache: `bool` default `True`
                Cache the file saved in the temp directory.
            storage_space: `str` default `app_storage`
                Internal Storage space.
                    1. "app_storage": For raw storage.
                    2. "golden": Data Model golden records.
                    3. "staging": Staging records path
                    4. "staging_master": Staging records from Master
                    5. "staging_rejected": Staging records from Rejected
                    6. "view": Data Model View records
                    7. "app": App  bucket
                    8. "golden_cds": CDS golden records
                    9. "staging_cds": Staging Intake.
            columns: `list` default `None`
                Columns to fetch when using `parquet=True`
            chunk_size: `int` default `None`
                The size of a chunk of data whenever iterating (in bytes).
                This must be a multiple of 256 KB per the API specification.
        Usage:

        Loading a local file in CDS.

        .. code:: python

            from pycarol import Carol, Storage
            import pandas as pd
            login = Carol()
            stg = Storage(login)

            path = stg.load(name='myfile.csv', format='file')
            pd.read_csv(path)

        Loading an object.

        .. code:: python

            from pycarol import Carol, Storage
            login = Carol()
            stg = Storage(login)

            my_dict = stg.load(name='myfile.json',  format='pickle')

        It works for `format=joblib` as well,

        .. code:: python

            from pycarol import Carol, Storage
            login = Carol()
            stg = Storage(login)
            my_dict = stg.load(name='myfile.json',  format='joblib')

        Loading a pandas DataFrame

        .. code:: python

            import pandas as pd
            from pycarol import Carol, Storage

            login = Carol()
            stg = Storage(login)

            df = stg.load(name='myfile.parquet', parquet=True)


        """
        return self.backend.load(name=name, format=format, parquet=parquet, cache=cache, storage_space=storage_space,
                                 columns=columns, chunk_size=chunk_size)

    def files_storage_list(self, prefix='pipeline/',  print_paths=False):
        """
        It will return all files in Carol data Storage (CDS).

        Args:

            prefix: `str`, default `pipeline/`
                prefix of the folder to filter the output.
            print_paths: `bool`, default `False`
                Print the tree structure of the files in CDS

        Returns: list of files paths.

        """

        return self.backend.files_storage_list(prefix=prefix, print_paths=print_paths)

    def exists(self, name):
        """

        Check if files exists in Carol Storage.

        Args:

            name: `str`
                Filename

        Returns: `bool`

        """
        return self.backend.exists(name)

    def delete(self, name):
        """

        Delete a file in Carol Storage.

        Args:

            name: `str`
                Filename to be deleted.

        Returns:

        """
        self.backend.delete(name)

    def build_url_parquet_golden(self, dm_name):
        return self.backend.build_url_parquet_golden(dm_name)

    def build_url_parquet_golden_cds(self, dm_name):
        return self.backend.build_url_dask_parquet_golden_cds(dm_name)

    def build_url_parquet_staging(self, staging_name, connector_id):
        return self.backend.build_url_parquet_staging(staging_name, connector_id)

    def build_url_parquet_staging_cds(self, staging_name, connector_id):
        return self.backend.build_url_dask_parquet_staging_cds(staging_name, connector_id)

    def build_url_parquet_staging_master(self, staging_name, connector_id):
        return self.backend.build_url_parquet_staging_master(staging_name, connector_id)

    def build_url_parquet_staging_rejected(self, staging_name, connector_id):
        return self.backend.build_url_parquet_staging_rejected(staging_name, connector_id)

    def build_url_parquet_golden_rejected_cds(self, dm_name):
        return self.backend.build_url_parquet_golden_rejected_cds(dm_name)

    def get_dask_options(self):
        return self.backend.get_dask_options()

    def get_golden_file_paths(self, dm_name):
        return self.backend.get_golden_file_paths(dm_name)

    def get_view_file_paths(self, view_name):
        return self.backend.get_view_file_paths(view_name)

    def get_staging_file_paths(self, staging_name, connector_id):
        return self.backend.get_staging_file_paths(staging_name, connector_id)

    def get_golden_rejected_cds_file_paths(self, dm_name, file_pattern=None):
        return self.backend.get_golden_rejected_cds_file_paths(dm_name, file_pattern=file_pattern)

    def get_staging_cds_file_paths(self, staging_name, connector_id, file_pattern=None):
        return self.backend.get_staging_cds_file_paths(staging_name, connector_id, file_pattern=file_pattern)

    def get_golden_cds_file_paths(self, dm_name, file_pattern=None):
        return self.backend.get_golden_cds_file_paths(dm_name, file_pattern=file_pattern)

    def get_view_cds_file_paths(self, dm_name):
        return self.backend.get_view_cds_file_paths(dm_name)
