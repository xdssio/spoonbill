import time
import typing

import redis

from spoonbill.datastores import KeyValueStore, Strict

REDIS_DEFAULT_HOST = 'localhost'
REDIS_DEFAULT_PORT = 6379
REDIS_DEFAULT_DB = 1


class RedisStore(KeyValueStore, Strict):
    """
    Redis Key-value store
    """

    def __init__(self, store: typing.Any, strict: bool = False):
        """

        :param store: The redis.Redis client
        :param strict: If False, all keys and values are encoded and decoded using cloudpickle - This make the RedisDict behave like a normal dict
                If True, all keys and values are encoded and decoded using str as default with redis - This make reading, writing and scaning faster
                default: False
        """
        self._store = store
        self.strict = strict
        self.as_string = True

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

    def scan(self, pattern: str = None, *args, **kwargs):
        params = {'count': 1000000}
        if pattern:
            params['match'] = pattern
        if len(self) > 0:
            cursor = '0'
            while cursor != 0:
                cursor, keys = self._store.scan(cursor=cursor, **params)
                values = self._store.mget(*keys)
                for key, value in zip(keys, values):
                    if value is None:
                        continue
                    yield self.decode_key(key), self.decode_value(value)

    def _items(self, *args, **kwargs):
        keys = self._store.keys(*args, **kwargs)
        if keys:
            values = self._store.mget(*keys)
            for key, value in zip(keys, values):
                yield self.decode_key(key), self.decode_value(value)
        return iter([])

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        if pattern:
            kwargs['pattern'] = pattern
        for i, key in enumerate(self._store.keys(*args, **kwargs)):
            if i == limit:
                break
            yield self.decode_key(key)

    def values(self, keys: str = None, limit: int = None, default=None, *args, **kwargs):
        if keys:
            pipeline = self.pipeline()
            for key in keys:
                pipeline.get(self.encode_key(key))
            for value in pipeline.execute():
                yield self.decode_value(value) if value is not None else default
        else:
            for key, value in self._items(*args, **kwargs):
                if value is None:
                    continue
                yield self.decode_value(value)

    def items(self, conditions: str = None, limit: int = None, *args, **kwargs):
        for key, value in self._scan_match(self._items(*args, **kwargs), conditions=conditions, limit=limit):
            yield key, value

    def _flush(self):
        count = len(self)
        self._store.flushdb()
        return count

    @classmethod
    def open(cls, url: str, strict: bool = False, **kwargs):
        kwargs['decode_responses'] = kwargs.get('decode_responses', True)
        return RedisStore(store=redis.Redis.from_url(url, **kwargs), strict=strict)

    @classmethod
    def from_connection(cls, host: str = REDIS_DEFAULT_HOST, port: int = REDIS_DEFAULT_PORT,
                        db: int = None, strict: bool = False, **kwargs):

        if db is None:
            store = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            db = len(RedisStore._databases_names(store))  # TODO test this
        kwargs['decode_responses'] = kwargs.get('decode_responses', True)
        return RedisStore(store=redis.Redis(host=host, port=port, db=db, **kwargs), strict=strict)

    @property
    def _backup_path(self):
        return self._get_path(self._store.config_get('dir')['dir']).joinpath(
            self._store.config_get('dbfilename')['dbfilename'])

    def save(self, path: str):
        self._store.bgsave()
        time.sleep(1)
        self._get_path(path).write_bytes(self._backup_path.read_bytes())

    def load(self, path: str):
        print(f"Loading for redis in not trivial")
        print("1. Stop redis server")
        print(f"2. Copy the file from {path} to {self._backup_path}")
        print("3. If AOF is enabled, turn it off")
        print("4. Start redis server")
        print("5. Set AOF to the desired state")
        print(
            "have a look here: https://www.digitalocean.com/community/tutorials/how-to-back-up-and-restore-your-redis-data-on-ubuntu-14-04#step-5-restoring-redis-database-from-backup")
        return None

    def encode_key(self, key):
        if self.strict or isinstance(key, str):
            return key
        return self.encode(key)
