import typing
import redis

from spoonbill.stores.base import KeyValueBase

REDIS_DEFAULT_HOST = 'localhost'
REDIS_DEFAULT_PORT = 6379
REDIS_DEFAULT_DB = 1


class RedisDict(KeyValueBase):

    def __init__(self, store: typing.Any, as_strings: bool = False):
        """

        :param store: The redis.Redis client
        :param as_strings: If False, all keys and values are encoded and decoded using cloudpickle - This make the RedisDict behave like a normal dict
                If True, all keys and values are encoded and decoded using str as default with redis - This make reading, writing and scaning faster
                default: False
        """
        self._store = store
        self.as_strings = as_strings

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

    def encode_key(self, key):
        if self.as_strings:
            return key
        elif isinstance(key, str):
            return key
        return self.encode(key)

    def decode_key(self, key):
        if self.as_strings:
            return key
        elif str(key)[:2] != "'b":
            return key
        return self.decode(key)

    def encode_value(self, value):
        if self.as_strings:  # let redis handle the encoding
            return value
        return self.encode(value)

    def decode_value(self, value):
        if self.as_strings:  # let redis handle the encoding
            return value
        return self.decode(value)

    def __setitem__(self, key, value):
        self._store[self.encode_key(key)] = self.encode_value(value)
        return value

    def __getitem__(self, item):
        return self.decode_value(self._store.get(self.encode_key(item)))

    def __contains__(self, item):
        return bool(self._store.exists(self.encode_key(item)))

    def __delitem__(self, key):
        return self._store.delete(self.encode_key(key))

    def pop(self, item, default=None):
        key = self.encode_key(item)
        value = self.decode_value(self._store.get(key)) or default
        self._store.delete(key)
        return value

    def get(self, key, default=None):
        return self.decode_value(self._store.get(self.encode_key(key))) or default

    def set(self, key, value):
        return self._store.set(self.encode_key(key), self.encode_value(value))

    def get_batch(self, keys, default=None):
        pipeline = self.pipeline()
        for key in keys:
            pipeline.get(self.encode_key(key))
        for value in pipeline.execute():
            yield self.decode_value(value) if value is not None else default

    def set_batch(self, keys, values):
        pipeline = self.pipeline()
        for key, value in zip(keys, values):
            pipeline.set(self.encode_key(key), self.encode_value(value))
        pipeline.execute()
        return True

    def update(self, d):
        pipeline = self._store.pipeline()
        for key, value in d.items():
            pipeline.set(self.encode_key(key), self.encode_value(value))
        pipeline.execute()
        return True

    def delete(self, key):
        return self._store.delete(self.encode_key(key))

    def pipeline(self):
        return self._store.pipeline()

    def values(self):
        if len(self) > 0:
            cursor = '0'
            while cursor != 0:
                cursor, keys = self._store.scan(cursor=cursor, count=1000000)
                for value in self._store.mget(*keys):
                    if value is None:
                        continue
                    yield self.decode_value(value)

    def items(self):
        keys = self._store.keys()
        if len(keys) == 0:
            return []
        values = self._store.mget(*keys)
        for key, value in zip(keys, values):
            if value is None:
                continue
            yield self.decode_key(key), self.decode_value(value)

    def _flush(self):
        return self._store.flushdb()

    def _flushall(self):
        return self._store.flushall()

    @classmethod
    def from_url(cls, url: str, as_strings: bool = False, **kwargs):
        kwargs['decode_responses'] = kwargs.get('decode_responses', True)
        return RedisDict(store=redis.Redis.from_url(url, **kwargs), as_strings=as_strings)

    @classmethod
    def from_connection(cls, host: str = REDIS_DEFAULT_HOST, port: int = REDIS_DEFAULT_PORT,
                        db: int = None, as_strings: bool = False, **kwargs):

        if db is None:
            store = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            db = len(RedisDict._databases_names(store))  # TODO test this
        kwargs['decode_responses'] = kwargs.get('decode_responses', True)
        return RedisDict(store=redis.Redis(host=host, port=port, db=db, **kwargs), as_strings=as_strings)

    def __repr__(self):
        size = len(self)
        items = str({key: value for i, (key, value) in enumerate(self.items()) if i < 5})[
                :-1] + '...' if size > 5 else str({key: value for key, value in self.items()})
        return f"RedisStore(host={self.host}, port={self.port}, db={self.db},string_keys={self.as_strings}) of size {size}\n{items}"

    def scan(self, *args, **kwargs):
        return self._store.scan_iter(*args, **kwargs)

    def keys(self, *args, **kwargs):
        for key in self._store.keys(*args, **kwargs):
            yield self.decode_key(key)
