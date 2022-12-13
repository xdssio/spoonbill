import warnings

import cloudpickle

from spoonbill.stores.base import KeyValueBase

REDIS_DEFAULT_HOST = 'localhost'
REDIS_DEFAULT_PORT = 6379
REDIS_DEFAULT_DB = 1


class RedisDict(KeyValueBase):

    def __len__(self):
        return self._store.dbsize()

    @property
    def host(self):
        return self._store.connection_pool.make_connection().host

    @property
    def port(self):
        return self._store.connection_pool.make_connection().port

    @property
    def db(self):
        return self._store.connection_pool.make_connection().db

    @classmethod
    def databases_count(cls, store):
        return store.config_get("databases")

    @classmethod
    def _databases_names(cls, store):
        return list(store.config_get("keyspace").keys())

    @staticmethod
    def encode(value):
        return str(cloudpickle.dumps(value))

    @staticmethod
    def decode(value):
        if value:
            return cloudpickle.loads(eval(value))

    def __setitem__(self, key, value):
        self._store[self.encode(key)] = self.encode(value)
        return value

    def __getitem__(self, item):
        return self.decode(self._store.get(self.encode(item)))

    def __contains__(self, item):
        return bool(self._store.exists(self.encode(item)))

    def __delitem__(self, key):
        return self._store.delete(self.encode(key))

    def pop(self, item, default=None):
        key = self.encode(item)
        value = self.decode(self._store.get(key)) or default
        self._store.delete(key)
        return value

    def get(self, key, default=None):
        return self.decode(self._store.get(self.encode(key))) or default

    def set(self, key, value):
        return self._store.set(self.encode(key), self.encode(value))

    def get_batch(self, keys, default=None):
        pipeline = self.pipeline()
        for key in keys:
            pipeline.get(self.encode(key))
        for value in pipeline.execute():
            yield self.decode(value) if value is not None else default

    def set_batch(self, keys, values):
        pipeline = self.pipeline()
        for key, value in zip(keys, values):
            pipeline.set(self.encode(key), self.encode(value))
        pipeline.execute()
        return True

    def update(self, d):
        pipeline = self._store.pipeline()
        for key, value in d.items():
            pipeline.set(self.encode(key), self.encode(value))
        pipeline.execute()
        return True

    def delete(self, key):
        return self._store.delete(self.encode(key))

    def pipeline(self):
        return self._store.pipeline()

    def keys(self, pattern: str = None, *args, **kwargs):
        if pattern:
            warnings.warn(
                "RedisStore.keys() does not support pattern argument as keys are encoded - try RedisStringDict")
        for key in self._store.keys(*args, **kwargs):
            yield self.decode(key)

    def scan(self, *args, **kwargs):
        raise NotImplementedError("RedisStore.scan() is not implemented as keys are encoded - try RedisStringDict")

    def values(self):
        if len(self) > 0:
            cursor = '0'
            while cursor != 0:
                cursor, keys = self._store.scan(cursor=cursor, count=1000000)
                for value in self._store.mget(*keys):
                    if value is None:
                        continue
                    yield self.decode(value)

    def items(self):
        keys = self._store.keys()
        if len(keys) == 0:
            return []
        values = self._store.mget(*keys)
        for key, value in zip(keys, values):
            if value is None:
                continue
            yield self.decode(key), self.decode(value)

    def _flush(self):
        return self._store.flushdb()

    def _flushall(self):
        return self._store.flushall()

    @classmethod
    def from_connection(cls, host: str = REDIS_DEFAULT_HOST, port: int = REDIS_DEFAULT_PORT,
                        db: int = None):
        import redis
        if db is None:
            store = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            db = len(RedisDict._databases_names(store))
        return RedisDict(store=redis.Redis(host=host, port=port, db=db, decode_responses=True))

    def __repr__(self):
        size = len(self)
        items = str({key: value for i, (key, value) in enumerate(self.items()) if i < 5})[
                :-1] + '...' if size > 5 else str({key: value for key, value in self.items()})
        return f"RedisStore(host={self.host}, port={self.port}, db={self.db}) of size {size}\n{items}"


class RedisStringDict(RedisDict):

    @staticmethod
    def encode(value):
        return value

    @staticmethod
    def decode(value):
        return value

    def __repr__(self):
        size = len(self)
        items = str({key: value for i, (key, value) in enumerate(self.items()) if i < 5})[
                :-1] + '...' if size > 5 else str({key: value for key, value in self.items()})
        return f"RedisStringStore(host={self.host}, port={self.port}, db={self.db}) of size {size}\n{items}"

    @classmethod
    def from_connection(cls, host: str = REDIS_DEFAULT_HOST, port: int = REDIS_DEFAULT_PORT,
                        db: int = None):
        import redis
        if db is None:
            store = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            db = len(RedisDict._databases_names(store))
        return RedisStringDict(store=redis.Redis(host=host, port=port, db=db, decode_responses=True))

    def scan(self, *args, **kwargs):
        return self._store.scan_iter(*args, **kwargs)

    def keys(self, *args, **kwargs):
        for key in self._store.keys(*args, **kwargs):
            yield self.decode(key)
