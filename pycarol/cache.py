from .utils.singleton import KeySingleton
import redis


class Cache(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

    def set(self, key, val):
        key = '{}-{}-{}'.format(self.carol.tenant['mdmId'], self.carol.app_name, key)
        self.r.set(key, val)

    def get(self, key):
        key = '{}-{}-{}'.format(self.carol.tenant['mdmId'], self.carol.app_name, key)
        return self.r.get(key)
