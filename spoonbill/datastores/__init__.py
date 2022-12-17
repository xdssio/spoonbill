import typing
import contextlib
import warnings

import cloudpickle
import re

KEY = 'key'
VALUE = 'value'
RANDOM_VALUE = '#5f1a7da3a2b04d629231108bb6548dcb#'


class Strict:
    encoding: str = 'cp1252'
    strict: bool = False

    @staticmethod
    def encode(value):
        return cloudpickle.dumps(value).decode(Strict.encoding)

    @staticmethod
    def decode(value):
        if value is not None:
            return cloudpickle.loads(value.encode(Strict.encoding))

    def encode_key(self, key):
        if self.strict:
            return key
        elif isinstance(key, str):
            return key
        return self.encode(key)

    @staticmethod
    def _is_encoded(value):
        return str(value)[0] == '€'

    def decode_key(self, key):
        if self.strict:
            return key
        elif self._is_encoded(key):
            return self.decode(key)
        return key

    def encode_value(self, value):
        if self.strict:  # let redis handle the encoding
            return value
        return self.encode(value)

    def decode_value(self, value):
        if self.strict:  # let redis handle the encoding
            return value
        return self.decode(value)

    def save(self, path):
        path = self._get_path(path)
        path.write_bytes(cloudpickle.dumps(self))
        return True

    @classmethod
    def from_file(cls, path):
        path = cls._get_path(path)
        return cloudpickle.loads(path.read_bytes())

    load = from_file


class KeyValueStore(Strict):
    _store: typing.Any = None
    strict: bool = False

    def __init__(self, store: typing.Any, strict: bool = False):
        self._store = store
        self.strict = strict

    def __getitem__(self, item):
        return self.decode_value(self._store[self.encode_key(item)])

    def __setitem__(self, key, value):
        self._store[self.encode_key(key)] = self.encode_value(value)

    def __len__(self):
        return len(self._store)

    def __iter__(self):
        return iter(self.keys())

    def __next__(self):
        for key in self.keys():
            yield key

    def __eq__(self, other):
        if len(self) != len(other):
            return False
        for key, value in self.items():
            if other.get(key) != value:
                return False
        return True

    def _to_iter(self, sequence: typing.Sequence, pattern: str = None, count: int = None):
        if pattern:
            pattern = re.compile(pattern)
        for i, item in enumerate(sequence):
            if i == count:
                break
            if not pattern:
                yield item
            else:
                if isinstance(item, tuple):
                    key = item[0]
                elif isinstance(item, dict):
                    key = item.get(KEY)
                else:
                    key = item
                key = self.decode_key(key)
                if pattern.match(str(key)):
                    yield item

    def keys(self, *args, **kwargs):
        for key in self._store.keys(*args, **kwargs):
            yield self.decode_key(key)

    def values(self):
        for value in self._store.values():
            yield self.decode_value(value)

    def items(self):
        for key, value in self._store.items():
            yield self.decode_key(key), self.decode_value(value)

    def pop(self, key, default=None):
        ret = self._store.pop(self.decode_key(key))
        if ret is None:
            return default
        return self.decode_value(ret)

    def popitem(self):
        return self.decode_value(self._store.popitem())

    def get(self, key, default=None):
        ret = self._store.get(self.encode_key(key))
        if ret is None:
            return default
        return self.decode_value(ret)

    def set(self, key, value):
        return self._store.set(self.encode_key(key), self.encode_value(value))

    def update(self, d):
        self._store.update({self.encode_key(key): self.encode_value(value) for key, value in d.items()})
        return self

    def get_batch(self, keys, default=None):
        for key in keys:
            ret = self.get(self.encode_key(key))
            if ret is None:
                yield default
            else:
                yield self.decode_value(ret)

    def set_batch(self, keys, values):
        for key, value in zip(keys, values):
            self.set(self.encode_key(key), self.encode_value(value))
        return True

    def scan(self, pattern: str = None, count: int = None, **kwargs):
        if count is not None:
            warnings.warn('count is not supported in MemoryStore')
        if not pattern:
            pattern = "[\s\S]*"
        pattern = re.compile(pattern)
        for key in self.keys():
            if pattern.match(str(key)):
                yield key

    def __repr__(self):
        size = len(self)
        items = str({key: value for i, (key, value) in enumerate(self.items()) if i < 5})[
                :-1] + '...' if size > 5 else str({key: value for key, value in self.items()})
        return f"{self.__class__.__name__}() of size {size}\n{items}"


class ContextStore(KeyValueStore, Strict):
    store_path: str = None
    manager: typing.Any = None
    open_params: dict = {}

    def __getitem__(self, item):
        with self.manager.open(self.store_path, **self.open_params) as store:
            return self.decode_value(store[self.encode_key(item)])

    def __setitem__(self, key, value):
        with self.manager.open(self.store_path, **self.open_params) as store:
            store[self.encode_key(key)] = self.encode_value(value)

    def __contains__(self, item):
        with self.manager.open(self.store_path, **self.open_params) as store:
            return self.encode_key(item) in store

    def __len__(self):
        with self.manager.open(self.store_path, **self.open_params) as store:
            return len(store)

    def keys(self, pattern: str = None, count: int = None, **kwargs):
        with self.manager.open(self.store_path, **self.open_params) as store:
            for key in self._to_iter(store.keys(), pattern=pattern, count=count):
                yield self.decode_key(key)

    def values(self):
        with self.manager.open(self.store_path, **self.open_params) as store:
            for value in store.values():
                yield self.decode_value(value)

    def items(self, pattern: str = None, count: int = None, **kwargs):
        with self.manager.open(self.store_path, **self.open_params) as store:
            for item in self._to_iter(store.items(), pattern=pattern, count=count):
                yield self.decode_key(item[0]), self.decode_value(item[1])

    def get(self, key, default=None):
        with self.manager.open(self.store_path, **self.open_params) as store:
            if key in store:
                return self.decode_value(store[self.encode_key(key)])
        return default

    def set(self, key, value):
        with self.manager.open(self.store_path, **self.open_params) as store:
            store[self.encode_key(key)] = self.encode_value(value)
        return True

    def pop(self, key, default=None):
        with self.manager.open(self.store_path, **self.open_params) as store:
            return self.decode_value(store.pop(self.encode_key(key), default))
        return default

    def popitem(self):
        with self.manager.open(self.store_path, **self.open_params) as store:
            return self.encode_value(store.popitem())

    def update(self, d):
        with self.manager.open(self.store_path, **self.open_params) as store:
            store.update({self.encode_key(key): self.encode_value(value) for key, value in d.items()})
        return self

    def get_batch(self, keys, default=None):
        with self.manager.open(self.store_path, **self.open_params) as store:
            for key in store.keys():
                yield self.decode_value(store.get(key, default))

    def set_batch(self, keys, values):
        with self.manager.open(self.store_path, **self.open_params) as store:
            for key, value in zip(keys, values):
                store[self.encode_key(key)] = self.encode_value(value)
        return True

    scan = items


from .inmemory import InMemoryDict
from .shelve import ShelveStore

with contextlib.suppress(ImportError):
    from .redis import RedisDict

with contextlib.suppress(ImportError):
    from .lmdb import LmdbDict

with contextlib.suppress(ImportError):
    from .pysos import PysosDict

with contextlib.suppress(ImportError):
    from .dynamodb import DynamoDBDict

with contextlib.suppress(ImportError):
    from .firestore import FireStoreDict

with contextlib.suppress(ImportError):
    from .buckets import BucketDict

with contextlib.suppress(ImportError):
    from .cosmos import CosmosDBDict

with contextlib.suppress(ImportError):
    from .mongodb import MongoDBDict