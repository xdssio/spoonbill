import json
import typing
import contextlib
import cloudpickle

REDIS_DEFAULT_HOST = 'localhost'
REDIS_DEFAULT_PORT = 6379
REDIS_DEFAULT_DB = 0


def to_int(i):
    with contextlib.suppress(ValueError):
        return int(i)
    return False


def to_float(f):
    with contextlib.suppress(ValueError):
        return float(f)
    return False


class KeyStore:
    _store: typing.Any = None

    def __getitem__(self, item):
        return self._store[item]

    def __setitem__(self, key, value):
        self._store[key] = value

    def __len__(self):
        return len(self._store)

    def __iter__(self):
        return iter(self._store)

    def keys(self):
        return self._store.keys()

    def values(self):
        return self._store.values()

    def pop(self, key, default=None):
        return self._store.pop(key, default)

    def popitem(self):
        return self._store.popitem()

    def get(self, key, default=None):
        return self._store.get(key, default)

    def set(self, key, value):
        return self._store.set(key, value)

    def update(self, d):
        self._store.update(d)
        return self


class MemoryKeyStore(dict, KeyStore):
    """
    A key-value store that stores everything in memory.
    Practically a python dictionary
    """

    @classmethod
    def from_dict(cls, d: dict):
        return MemoryKeyStore(d)

    @classmethod
    def from_json(cls, j):
        return MemoryKeyStore(json.loads(j))


a = MemoryKeyStore.from_json('{"a": 1}')
a.update({"b": 2})


class LmdbStore(KeyStore):

    def from_db(self, db_path):
        import lmdbm

        class JsonLmdb(lmdbm.Lmdb):
            def _pre_key(self, value):
                return value.encode("utf-8")

            def _post_key(self, value):
                return value.decode("utf-8")

            def _pre_value(self, value):
                return json.dumps(value).encode("utf-8")

            def _post_value(self, value):
                return json.loads(value.decode("utf-8"))

        self._store = JsonLmdb.open(db_path, "c")


class SQLliteStore(KeyStore):

    def from_db(self, db_path):
        import sqlitedict
        self._store = sqlitedict.SqliteDict(db_path, autocommit=True)

    def close(self):
        self._store.close()


class ShelveStore(KeyStore):

    def from_db(self, db_path):
        import shelve
        self._store = shelve.open(db_path, 'c')


class RedisStore(KeyStore):

    def __init__(self, store):
        self._store = store

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

    def pop(self, item, default=None):
        key = self.encode(item)
        value = self.decode(self._store.get(key)) or default
        self._store.delete(key)
        return value

    def get(self, key, default=None):
        return self.decode(self._store.get(key)) or default

    def set(self, key, value):
        return self._store.set(self.encode_key(key), self.decode(value))

    def update(self, d):
        for key, value in d.items():
            self.set(key, value)
        return self

    def keys(self):
        for key in self._store.keys():
            yield self.decode(key)

    def values(self):
        for value in self._store.values():
            yield self.decode(value)

    def values(self):
        data = []
        keys = self._store.keys()
        if len(keys) == 0:
            return data
        values = self._store.mget(*keys)
        for value in values:
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

    @classmethod
    def from_connection(cls, host: str = REDIS_DEFAULT_HOST, port: int = REDIS_DEFAULT_PORT,
                        db: int = None):
        import redis
        if db is None:
            store = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            db = len(RedisStore._databases_names(store))
        return RedisStore(store=redis.Redis(host=host, port=port, db=db, decode_responses=True))
