import os
import pickle
import gzip
import pandas as pd
from .. import __TEMP_STORAGE__


class StorageGCPCS:
    def __init__(self, carol, carolina):
        self.carol = carol
        self.carolina = carolina
        self.client = None
        self.bucket_app_storage = None

    def _init_if_needed(self):
        if self.client is not None:
            return

        self.client = self.carolina.get_client()
        self.bucket_app_storage = self.client.bucket(self.carolina.cds_app_storage_path['bucket'])
        if not os.path.exists(__TEMP_STORAGE__):
            os.makedirs(__TEMP_STORAGE__)

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        remote_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        bucket = self.bucket_app_storage

        if parquet:
            if not isinstance(obj, pd.DataFrame):
                raise ValueError("Object to be saved as parquet must be a DataFrame")
            obj.to_parquet(local_file_name)
        elif format == 'joblib':
            import joblib

            if not cache:
                from io import BytesIO
                with BytesIO() as buffer:
                    joblib.dump(obj, buffer)
                    buffer.seek(0)
                    bucket.upload_fileobj(buffer, remote_file_name)
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

        blob = bucket.blob(remote_file_name)

        blob.upload_from_filename(filename=local_file_name)
        os.utime(local_file_name, None)

    def load(self, name, format='pickle', parquet=False, cache=True, storage_space='app_storage', columns=None):
        self._init_if_needed()
        if storage_space == 'app_storage':
            remote_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"
            bucket = self.bucket_app_storage
        else:
            remote_file_name = name
            bucket = self.client.bucket(self.carolina.get_bucket_name(storage_space))

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        has_cache = cache and os.path.isfile(local_file_name)

        if has_cache:
            localts = os.stat(local_file_name).st_mtime
        else:
            localts = 0

        blob = bucket.blob(remote_file_name)
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
        self._init_if_needed()
        remote_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"
        print(remote_file_name)

        blob = self.bucket_app_storage.blob(remote_file_name)
        return blob.exists()

    def delete(self, name):
        self._init_if_needed()
        remote_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"

        blob = self.bucket_app_storage.blob(remote_file_name)
        blob.delete()

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

    def build_url_parquet_golden(self, dm_name):
        path = self.carolina.get_path("golden", {'dm_name': dm_name})
        return f'gcs://{self.carolina.get_bucket_name("golden")}/{path}'

    def build_url_parquet_staging(self, staging_name, connector_id):
        self._init_if_needed()
        path = self.carolina.get_path("staging", {'connector_id': connector_id, 'staging_type': staging_name})
        bucket_name = self.carolina.get_bucket_name("staging")
        bucket = self.client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=path))
        if len(blobs) == 0:
            return None
        return f'gcs://{bucket_name}/{path}'

    def build_url_parquet_staging_master(self, staging_name, connector_id):
        self._init_if_needed()
        path = self.carolina.get_path("staging_master", {'connector_id': connector_id, 'staging_type': staging_name})
        bucket_name = self.carolina.get_bucket_name("staging_master")
        bucket = self.client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=path))
        if len(blobs) == 0:
            return None
        return f'gcs://{bucket_name}/{path}'

    def build_url_parquet_staging_rejected(self, staging_name, connector_id):
        self._init_if_needed()
        path = self.carolina.get_path("staging_rejected", {'connector_id': connector_id, 'staging_type': staging_name})
        bucket_name = self.carolina.get_bucket_name("staging_rejected")
        bucket = self.client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=path))
        if len(blobs) == 0:
            return None
        return f'gcs://{bucket_name}/{path}'

    def get_dask_options(self):
        return {'token': self.carolina.token}

    def get_golden_file_paths(self, dm_name):
        self._init_if_needed()
        bucket = self.client.bucket(self.carolina.get_bucket_name('golden'))
        path = self.carolina.get_path('golden', {'dm_name': dm_name})
        blobs = bucket.list_blobs(prefix=path, delimiter=None)
        return [{'storage_space': 'golden', 'name': i.name} for i in blobs if i.name.endswith('.parquet')]

    def get_staging_file_paths(self, staging_name, connector_id):
        self._init_if_needed()

        bucket_staging = self.client.bucket(self.carolina.get_bucket_name('staging'))
        path_staging = self.carolina.get_path("staging", {'connector_id': connector_id, 'staging_type': staging_name})
        blobs_staging = list(bucket_staging.list_blobs(prefix=path_staging))

        bucket_master = self.client.bucket(self.carolina.get_bucket_name('staging_master'))
        path_master = self.carolina.get_path("staging_master", {'connector_id': connector_id, 'staging_type': staging_name})
        blobs_master = list(bucket_master.list_blobs(prefix=path_master))

        bucket_rejected = self.client.bucket(self.carolina.get_bucket_name('staging_rejected'))
        path_rejected = self.carolina.get_path("staging_rejected", {'connector_id': connector_id, 'staging_type': staging_name})
        blobs_rejected = list(bucket_rejected.list_blobs(prefix=path_rejected))

        bs = [{'storage_space': 'staging', 'name': i.name} for i in blobs_staging if i.name.endswith('.parquet')]
        bm = [{'storage_space': 'staging_master', 'name': i.name} for i in blobs_master if i.name.endswith('.parquet')]
        br = [{'storage_space': 'staging_rejected', 'name': i.name} for i in blobs_rejected if i.name.endswith('.parquet')]

        ball = bs + bm + br
        return ball
