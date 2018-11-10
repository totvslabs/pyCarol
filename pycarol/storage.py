import os
import pickle
import calendar
import gzip
import pandas as pd
from multiprocessing import Process
from pycarol.carol_cloner import Cloner
from pycarol.utils.singleton import KeySingleton
from pycarol.carolina import Carolina
import botocore
from . import __BUCKET_NAME__, __TEMP_STORAGE__

class Storage(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.s3 = None
        self.bucket = None

    def _init_if_needed(self):
        if self.s3 is not None:
            return

        self.s3 = Carolina(self.carol).get_s3()
        self.bucket = self.s3.Bucket(__BUCKET_NAME__)
        if not os.path.exists(__TEMP_STORAGE__):
            os.makedirs(__TEMP_STORAGE__)

    def save_async(self, name, obj):
        p = Process(target=_save_async, args=(Cloner(self.carol), name, obj))
        p.start()

        return p

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        s3_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__,s3_file_name.replace("/", "-"))

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
                    self.bucket.upload_fileobj(buffer, s3_file_name)
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

        self.bucket.upload_file(local_file_name, s3_file_name)
        os.utime(local_file_name, None)

    def load(self, name, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        s3_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__,s3_file_name.replace("/", "-"))

        obj = self.bucket.Object(s3_file_name)
        if obj is None:
            return None

        has_cache = cache and os.path.isfile(local_file_name)

        if has_cache:
            localts = os.stat(local_file_name).st_mtime
        else:
            localts = 0

        try:
            s3ts = calendar.timegm(obj.last_modified.timetuple())
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None

        if not cache and format == 'joblib':
            import joblib
            from io import BytesIO
            with BytesIO() as data:
                self.bucket.download_fileobj(s3_file_name, data)
                data.seek(0)
                return joblib.load(data)

        # Local cache is outdated
        if localts < s3ts:
            self.bucket.download_file(s3_file_name, local_file_name)

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
        s3_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"

        obj = self.bucket.Object(s3_file_name)
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
        s3_file_name = f"storage/{self.carol.tenant['mdmId']}/{self.carol.app_name}/files/{name}"
        obj = self.bucket.Object(s3_file_name)
        if obj is not None:
            obj.delete()

        local_file_name = os.path.join(__TEMP_STORAGE__,s3_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)


def _save_async(cloner, name, obj):
    return Storage(cloner.build()).storage.save(name, obj)