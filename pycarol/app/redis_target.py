import redis
import os

class RedisTarget:
    """ Save temporary data in Redis.
        When Redis environment variables are undefined, it will save the data in Memory.

    Usage:
        from pycarol.carol import *
        from pycarol.auth.PwdAuth import *
        from pycarol.redis_target import RedisTarget

        carol = Carol('rui', 'rui', auth=PwdAuth('pedro.buzzi@totvs.com.br', 'Adsl0223'))

        m1 = RedisTarget(carol=carol)
        m1.save('key1', 'value 1')
        print (m1.load('key1'))
    """

    def __init__(self, carol=None, redis_host=None, redis_port=None, redis_password=None):
        if not redis_host:
            redis_host = os.environ.get('REDIS_SERVICE_HOST')
            redis_port = os.environ.get('REDIS_SERVICE_PORT')
            redis_password = os.environ.get('REDIS_SERVICE_PASSWORD', '')

        if redis_host and redis_port:
            self.target = _Redis(carol.domain, carol.app_name, redis_host, redis_port, redis_password)
        else:
            self.target = _Memory()

    def exists(self, name):
        return self.target.exists(name)

    def save(self, name, obj):
        self.target.save(name, obj)

    def load(self, name):
        return self.target.load(name)

    def remove(self, name):
        self.target.remove(name)


class _Redis:
    def __init__(self, domain, app_name, redis_host, redis_port, redis_password):
        self.r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.hash = '{}/{}'.format (domain, app_name)

    def exists(self, key):
        return self.r.hget(self.hash, key) is not None

    def save(self, key, obj):
        self.r.hset(self.hash, key, obj)

    def load(self, key):
        return self.r.hget(self.hash, key)

    def remove(self, key):
        self.r.hdel(self.hash, key)


class _Memory:
    def __init__(self):
        self.mem_data = {}

    def exists(self, key):
        return key in self.mem_data

    def save(self, key, obj):
        self.mem_data[key] = obj

    def load(self, key):
        return self.mem_data[key]

    def remove(self, key):
        self.mem_data.pop(key, None)
