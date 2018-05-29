import os
import pickle
import calendar
import gzip


class Storage:
    def __init__(self, carol):
        self.carol = carol
        self.s3 = None
        self.bucket = None

    def _init_if_needed(self):
        if self.s3 is not None:
            return

        self.s3 = self.carol.carolina.get_s3()
        self.bucket = self.s3.Bucket("carolina-dev-ca-central-1")
        if not os.path.exists('/tmp/carolina/cache'):
            os.makedirs('/tmp/carolina/cache')

    def save(self, name, obj):
        self._init_if_needed()
        s3_file_name = '{}/files/{}/{}'.format(self.carol.tenant['mdmId'], self.carol.app_name, name)
        local_file_name = '/tmp/carolina/cache/' + s3_file_name.replace("/", "-")
        with gzip.open(local_file_name, 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

        self.bucket.upload_file(local_file_name, s3_file_name)
        os.utime(local_file_name, None)

    def load(self, name):
        self._init_if_needed()
        s3_file_name = '{}/files/{}/{}'.format(self.carol.tenant['mdmId'], self.carol.app_name, name)
        local_file_name = '/tmp/carolina/cache/' + s3_file_name.replace("/", "-")

        obj = self.bucket.Object(s3_file_name)
        if obj is None:
            return None

        localts = os.stat(local_file_name).st_mtime
        s3ts = calendar.timegm(obj.last_modified.timetuple())

        # Local cache is outdated
        if localts < s3ts:
            self.bucket.download_file(s3_file_name, local_file_name)

        with gzip.open(local_file_name, 'rb') as f:
            return pickle.load(f)
