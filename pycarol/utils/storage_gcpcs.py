import os
import pickle
import gzip
from .. import __TEMP_STORAGE__
from collections import defaultdict
from ..utils.miscellaneous import prettify_path, _attach_path, _FILE_MARKER
from ._cds_utils import retry_check_sum


class StorageGCPCS:
    def __init__(self, carol, carolina):
        import pandas as pd
        self.carol = carol
        self.carolina = carolina


        if not os.path.exists(__TEMP_STORAGE__):
            os.makedirs(__TEMP_STORAGE__)

    def _get_app_storage_bucket(self):
        return self.carolina.get_client().bucket(self.carolina.get_bucket_name("app"))

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
        """

        remote_file_name = f"{self.carolina.get_path('app', {})}{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        bucket = self._get_app_storage_bucket()
        blob = bucket.blob(remote_file_name, chunk_size=chunk_size)

        if parquet:
            import pandas as pd
            if not isinstance(obj, pd.DataFrame):
                raise ValueError(f"Object to be saved as parquet must be a "
                                 f"DataFrame. Received a {type(obj)}")
            obj.to_parquet(local_file_name)
        elif format == 'joblib':
            import joblib

            if not cache:
                from io import BytesIO
                with BytesIO() as buffer:
                    joblib.dump(obj, buffer)
                    buffer.seek(0)
                    blob.upload_from_file(buffer)
                return
            else:
                joblib.dump(obj, local_file_name)
        elif format == 'pickle':
            with gzip.open(local_file_name, 'wb') as f:
                pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        elif format == 'file':
            local_file_name = obj
        else:
            raise ValueError("Supported formats are pickle, joblib or file")

        blob.upload_from_filename(filename=local_file_name)
        os.utime(local_file_name, None)

    @retry_check_sum
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

        """

        if storage_space == 'app_storage':
            remote_file_name = f"{self.carolina.get_path('app', {})}{name}"
            bucket = self._get_app_storage_bucket()
        else:
            remote_file_name = name
            bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name(storage_space))

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        has_cache = cache and os.path.isfile(local_file_name)

        if has_cache:
            localts = os.stat(local_file_name).st_mtime
        else:
            localts = 0

        blob = bucket.blob(remote_file_name, chunk_size=chunk_size)
        if not blob.exists():
            return None

        blob.reload()
        remote_ts = blob.updated.timestamp()

        if not cache and format == 'raw':
            import joblib
            from io import BytesIO
            buffer = BytesIO()
            blob.download_to_file(buffer)
            return buffer

        elif not cache and format == 'joblib':
            import joblib
            from io import BytesIO
            with BytesIO() as buffer:
                blob.download_to_file(buffer)
                return joblib.load(buffer)

        # Local cache is outdated
        if localts < remote_ts:
            blob.download_to_filename(local_file_name)

        if os.path.isfile(local_file_name):
            if parquet:
                import pandas as pd
                return pd.read_parquet(local_file_name, columns=columns)
            elif format == 'joblib':
                import joblib
                return joblib.load(local_file_name)
            elif format == 'pickle':
                with gzip.open(local_file_name, 'rb') as f:
                    return pickle.load(f)
            elif format == 'file':
                return local_file_name
            else:
                raise ValueError("Supported formats are pickle, joblib or file")
        else:
            return None

    def exists(self, name):
        remote_file_name = f"{self.carolina.get_path('app', {})}{name}"

        blob = self._get_app_storage_bucket().blob(remote_file_name)
        return blob.exists()

    def delete(self, name):
        remote_file_name = f"{self.carolina.get_path('app', {})}{name}"

        blob = self._get_app_storage_bucket().blob(remote_file_name)
        if blob.exists():
            blob.delete()

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

    def build_url_parquet_golden(self, dm_name):
        path = self.carolina.get_path("golden", {'dm_name': dm_name})
        return f'gcs://{self.carolina.get_bucket_name("golden")}/{path}'

    def build_url_parquet_golden_cds(self, dm_name):
        path = self.carolina.get_path("golden_cds", {'dm_name': dm_name})
        return f'gcs://{self.carolina.get_bucket_name("golden_cds")}/{path}'

    def build_url_dask_parquet_golden_cds(self, dm_name):
        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('golden_cds'))
        path = self.carolina.get_path("golden_cds", {'dm_name': dm_name})
        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        #Dask does not accept iterators.
        return [f'gcs://{file.bucket.name}/{file.name}' for file in blobs]

    def build_url_parquet_staging_cds(self, staging_name, connector_id):
        path = self.carolina.get_path('staging_cds', {'connector_id': connector_id, 'staging_type': staging_name})
        return f'gcs://{self.carolina.get_bucket_name("staging_cds")}/{path}'

    def build_url_dask_parquet_staging_cds(self, staging_name, connector_id):

        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('staging_cds'))
        path = self.carolina.get_path('staging_cds', {'connector_id': connector_id, 'staging_type': staging_name})
        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        #Dask does not accept iterators.
        return [f'gcs://{file.bucket.name}/{file.name}' for file in blobs]

    def build_url_parquet_view(self, view_name):
        path = self.carolina.get_path("view", {'relationship_view_name': view_name})
        return f'gcs://{self.carolina.get_bucket_name("view")}/{path}'

    def build_url_parquet_staging(self, staging_name, connector_id):
        path = self.carolina.get_path("staging", {'connector_id': connector_id, 'staging_type': staging_name})
        bucket_name = self.carolina.get_bucket_name("staging")
        bucket = self.carolina.get_client().bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=path))
        if len(blobs) == 0:
            return None
        return f'gcs://{bucket_name}/{path}'

    def build_url_parquet_staging_master(self, staging_name, connector_id):
        path = self.carolina.get_path("staging_master", {'connector_id': connector_id, 'staging_type': staging_name})
        bucket_name = self.carolina.get_bucket_name("staging_master")
        bucket = self.carolina.get_client().bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=path))
        if len(blobs) == 0:
            return None
        return f'gcs://{bucket_name}/{path}'

    def build_url_parquet_staging_rejected(self, staging_name, connector_id):
        path = self.carolina.get_path("staging_rejected", {'connector_id': connector_id, 'staging_type': staging_name})
        bucket_name = self.carolina.get_bucket_name("staging_rejected")
        bucket = self.carolina.get_client().bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=path))
        if len(blobs) == 0:
            return None
        return f'gcs://{bucket_name}/{path}'

    def get_dask_options(self):
        return {'token': self.carolina.token}

    # TODO All of these could be a single function.
    def get_golden_file_paths(self, dm_name):
        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('golden'))
        path = self.carolina.get_path('golden', {'dm_name': dm_name})
        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        return ({'storage_space': 'golden', 'name': i.name} for i in blobs if i.name.endswith('.parquet'))

    def get_golden_cds_file_paths(self, dm_name, file_pattern=None):
        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('golden_cds'))
        path = self.carolina.get_path('golden_cds', {'dm_name': dm_name})
        if file_pattern is not None:
            path = path + file_pattern

        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        return ({'storage_space': 'golden_cds', 'name': i.name} for i in blobs if i.name.endswith('.parquet'))

    def get_view_cds_file_paths(self, dm_name):
        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('view_cds'))
        path = self.carolina.get_path('view_cds', {'dm_name': dm_name})
        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        return ({'storage_space': 'view_cds', 'name': i.name} for i in blobs if i.name.endswith('.parquet'))

    def get_view_file_paths(self, view_name):
        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('view'))
        path = self.carolina.get_path('view', {'relationship_view_name': view_name})
        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        return ({'storage_space': 'view', 'name': i.name} for i in blobs if i.name.endswith('.parquet'))

    def get_staging_cds_file_paths(self, staging_name, connector_id, file_pattern=None):

        bucket = self.carolina.get_client().bucket(self.carolina.get_bucket_name('staging_cds'))
        path = self.carolina.get_path('staging_cds', {'connector_id': connector_id, 'staging_type': staging_name})
        if file_pattern is not None:
            path = path + file_pattern

        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        return ({'storage_space': 'staging_cds', 'name': i.name} for i in blobs if i.name.endswith('.parquet'))

    def get_staging_file_paths(self, staging_name, connector_id):
        bucket_staging = self.carolina.get_client().bucket(self.carolina.get_bucket_name('staging'))
        path_staging = self.carolina.get_path("staging", {'connector_id': connector_id, 'staging_type': staging_name})
        blobs_staging = list(bucket_staging.list_blobs(prefix=path_staging))

        bucket_master = self.carolina.get_client().bucket(self.carolina.get_bucket_name('staging_master'))
        path_master = self.carolina.get_path("staging_master",
                                             {'connector_id': connector_id, 'staging_type': staging_name})
        blobs_master = list(bucket_master.list_blobs(prefix=path_master))

        bucket_rejected = self.carolina.get_client().bucket(self.carolina.get_bucket_name('staging_rejected'))
        path_rejected = self.carolina.get_path("staging_rejected",
                                               {'connector_id': connector_id, 'staging_type': staging_name})
        blobs_rejected = list(bucket_rejected.list_blobs(prefix=path_rejected))

        bs = [{'storage_space': 'staging', 'name': i.name} for i in blobs_staging if i.name.endswith('.parquet')]
        bm = [{'storage_space': 'staging_master', 'name': i.name} for i in blobs_master if i.name.endswith('.parquet')]
        br = [{'storage_space': 'staging_rejected', 'name': i.name} for i in blobs_rejected if
              i.name.endswith('.parquet')]

        ball = bs + bm + br
        return ball

    def files_storage_list(self, prefix='pipeline/', print_paths=False):
        bucket_staging = self.carolina.get_client().bucket(self.carolina.get_bucket_name('app'))
        path_app = self.carolina.get_path('app', {})

        files = list(bucket_staging.list_blobs(prefix=path_app + prefix))
        files = [i.name.split(path_app)[-1] for i in files]
        if print_paths:
            main_dict = defaultdict(dict, ((_FILE_MARKER, []),))
            for line in files:
                _attach_path(line, main_dict)
            prettify_path(main_dict)

        return files
