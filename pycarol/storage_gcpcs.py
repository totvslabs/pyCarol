import os
import pickle
import calendar
import gzip
import pandas as pd
from pycarol.utils.singleton import KeySingleton
from . import __TEMP_STORAGE__


class StorageGCPCS:
    def __init__(self, carol, carolina):
        self.carol = carol
        self.carolina = carolina
        self.client = None
        self.bucket = None

    def _init_if_needed(self):
        if self.client is not None:
            return

        self.client = self.carolina.get_client()
        self.bucket = self.client.bucket(self.carolina.bucketName)
        if not os.path.exists(__TEMP_STORAGE__):
            os.makedirs(__TEMP_STORAGE__)

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        remote_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

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
                    self.bucket.upload_fileobj(buffer, remote_file_name)
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

        blob = self.bucket.blob(remote_file_name)

        blob.upload_from_filename(filename=local_file_name)
        os.utime(local_file_name, None)

    def load(self, name, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        remote_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))

        has_cache = cache and os.path.isfile(local_file_name)

        if has_cache:
            localts = os.stat(local_file_name).st_mtime
        else:
            localts = 0

        s3ts = 0
        #try:
        #    s3ts = calendar.timegm(obj.last_modified.timetuple())
        #except botocore.exceptions.ClientError as e:
        #    if e.response['Error']['Code'] == "404":
        #        return None

        if not cache and format == 'joblib':
            import joblib
            from io import BytesIO
            with BytesIO() as data:
                self.bucket.download_fileobj(remote_file_name, data)
                data.seek(0)
                return joblib.load(data)

        # Local cache is outdated
        if localts < s3ts:
            blob = self.bucket.blob(remote_file_name)
            blob.download_to_file(local_file_name)

        if os.path.isfile(local_file_name):
            if parquet:
                return pd.read_parquet(local_file_name)
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
        remote_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"

        obj = self.bucket.Object(remote_file_name)
        if obj is None:
            return False

        try:
            calendar.timegm(obj.last_modified.timetuple())
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

        return True

    def delete(self, name):
        self._init_if_needed()
        remote_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"
        obj = self.bucket.Object(remote_file_name)
        if obj is not None:
            obj.delete()

        local_file_name = os.path.join(__TEMP_STORAGE__, remote_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)
