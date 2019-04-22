import os
import pickle
import calendar
import gzip
import pandas as pd
import botocore
from .. import __TEMP_STORAGE__


class StorageAWSS3:
    def __init__(self, carol, carolina):
        self.carol = carol
        self.client = None
        self.bucket = None
        self.carolina = carolina

    def _init_if_needed(self):
        if self.client is not None:
            return

        self.client = self.carolina.get_client()
        self.bucket = self.client.Bucket(self.carolina.cds_app_storage_path['bucket'])
        if not os.path.exists(__TEMP_STORAGE__):
            os.makedirs(__TEMP_STORAGE__)

    def save(self, name, obj, format='pickle', parquet=False, cache=True):
        self._init_if_needed()
        s3_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"
        local_file_name = os.path.join(__TEMP_STORAGE__, s3_file_name.replace("/", "-"))

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

    def load(self, name, format='pickle', parquet=False, cache=True, storage_space='app_storage', columns=None):
        self._init_if_needed()
        if storage_space == 'app_storage':
            s3_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"
        else:
            s3_file_name = name
        local_file_name = os.path.join(__TEMP_STORAGE__,s3_file_name.replace("/", "-"))

        obj = self.bucket.Object(s3_file_name)
        if obj is None:
            return None

        if not cache and format == 'raw':
            from io import BytesIO
            buffer = BytesIO()
            self.bucket.download_fileobj(s3_file_name, buffer)
            return buffer

        elif not cache and format == 'joblib':
            import joblib
            from io import BytesIO
            with BytesIO() as data:
                self.bucket.download_fileobj(s3_file_name, data)
                data.seek(0)
                return joblib.load(data)

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

        # Local cache is outdated
        if localts < s3ts:
            self.bucket.download_file(s3_file_name, local_file_name)

        if os.path.isfile(local_file_name):
            if parquet:
                return pd.read_parquet(local_file_name,columns=columns)
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
        s3_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"

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
        s3_file_name = f"{self.carolina.cds_app_storage_path['path']}/{name}"
        obj = self.bucket.Object(s3_file_name)
        if obj is not None:
            obj.delete()

        local_file_name = os.path.join(__TEMP_STORAGE__,s3_file_name.replace("/", "-"))
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

    # TODO: Use the path from caroline
    def build_url_parquet_golden(self, dm_name):
        tenant_id = self.carol.tenant['mdmId']
        return f's3://{self.carolina.cds_app_storage_path["bucket"]}/carol_export/{tenant_id}/{dm_name}/golden'

    def build_url_parquet_staging(self, staging_name, connector_id):
        tenant_id = self.carol.tenant['mdmId']
        return f's3://{self.carolina.cds_app_storage_path["bucket"]}/carol_export/{tenant_id}/{connector_id}_{staging_name}/staging'

    def build_url_parquet_staging_master(self, staging_name, connector_id):
        tenant_id = self.carol.tenant['mdmId']
        return f's3://{self.carolina.cds_app_storage_path["bucket"]}/carol_export/{tenant_id}/{connector_id}_{staging_name}/master_staging'

    def build_url_parquet_staging_rejected(self, staging_name, connector_id):
        tenant_id = self.carol.tenant['mdmId']
        return f's3://{self.carolina.cds_app_storage_path["bucket"]}/carol_export/{tenant_id}/{connector_id}_{staging_name}/rejected_staging'

    def get_dask_options(self):
        return {"key": self.carolina.token['aiAccessKeyId'],
                "secret": self.carolina.token['aiSecretKey'],
                "token": self.carolina.token['aiAccessToken']}

    def get_golden_file_paths(self, dm_name):
        self._init_if_needed()
        tenant_id = self.carol.tenant['mdmId']
        template = dict(
            dm_name=dm_name,
            tenant_id=tenant_id
        )
        parq = list(
            self.bucket.objects.filter(Prefix=self.carolina.cds_golden_path['path'].format(**template))
        )
        return [{'storage_space': 'golden', 'name': i.key} for i in parq if i.key.endswith('.parquet')]

    def get_staging_file_paths(self, staging_name, connector_id):
        self._init_if_needed()
        tenant_id = self.carol.tenant['mdmId']
        template = dict(
            staging_type=staging_name,
            connector_id=connector_id,
            tenant_id=tenant_id
        )
        parq_stag = list(
            self.bucket.objects.filter(Prefix=self.carolina.cds_staging_path['path'].format(**template))
        )
        parq_master = list(
            self.bucket.objects.filter(Prefix=self.carolina.cds_staging_master_path['path'].format(**template))
        )
        parq_stag_rejected = list(
            self.bucket.objects.filter(Prefix=self.carolina.cds_staging_rejected_path['path'].format(**template))
        )

        bs = [{'storage_space': 'staging', 'name': i.key} for i in parq_stag if i.key.endswith('.parquet')]
        bm = [{'storage_space': 'staging_master', 'name': i.key} for i in parq_master if i.key.endswith('.parquet')]
        br = [{'storage_space': 'staging_rejected', 'name': i.key} for i in parq_stag_rejected if i.key.endswith('.parquet')]

        return bs + bm + br
