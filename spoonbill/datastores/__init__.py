import typing
import contextlib
import warnings

import cloudpickle
import re
import pathlib

KEY = 'key'
VALUE = 'VALUE#'
RANDOM_VALUE = '#5f1a7da3a2b04d629231108bb6548dcb#'


class Strict:
    encoding: str = 'cp1252'
    strict: bool = False
    as_string: bool = True

    def _is_encoded(self, value):
        if self.as_string:
            return isinstance(value, str) and str(value)[:3] in ('b"\\', "b'\\")
        return isinstance(value, bytes) and value[:1] == b'\x80'

    def encode(self, value):
        """Encode a value to a string.
        Note were using the str instead of encoding to bytes because it works best with
        numbers too without creating issues with different encodings.
        """
        encoded = cloudpickle.dumps(value)
        if self.as_string:
            return str(encoded)
        return encoded

    def decode(self, value):
        if value is not None:
            if self._is_encoded(value):
                if self.as_string:
                    value = eval(value)
                value = cloudpickle.loads(value)
                return value
            return value

    def encode_key(self, key):
        if self.strict:
            return key
        return self.encode(key)

    def decode_key(self, key):
        if self.strict:
            return key
        return self.decode(key)

    def encode_value(self, value):
        if self.strict:
            return value
        return self.encode(value)

    def decode_value(self, value):
        if self.strict:
            return value
        return self.decode(value)

    def save(self, path):
        path = self._get_path(path)
        path.write_bytes(cloudpickle.dumps(self))
        return True

    @staticmethod
    def _is_cloud_url(path):
        if hasattr(path, 'dirname'):
            path = path.dirname
        return re.match("^s3:\/\/|^az:\/\/|^gs:\/\/", path) is not None

    @classmethod
    def _get_path(cls, path):
        if cls._is_cloud_url(path):
            import cloudpathlib
            return cloudpathlib.CloudPath(path)
        return pathlib.Path(path)

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

    def _from_item(self, item):
        if item:
            return self.decode_key(item[0]), self.decode_value(item[1])

    def _scan_match(self, sequence: typing.Sequence, patterns: dict = None, limit: int = None):
        patterns = patterns or {}
        filters = []
        for feature, pattern in patterns.items():
            if isinstance(pattern, str):
                re_pattern = re.compile(pattern)
                validate = lambda x: re_pattern.match(str(x.get(feature)))
            else:
                validate = lambda x: x.get(feature) == pattern
            filters.append(validate)

        for i, item in enumerate(sequence):
            if i == limit:
                break
            key, value = self._from_item(item)
            if not patterns:
                yield key, value
            else:
                new_item = {KEY: key}
                if isinstance(value, dict):
                    new_item.update(value)
                else:
                    new_item[VALUE] = value

                if all([validate(new_item) for validate in filters]):
                    yield key, value

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        patterns = {KEY: pattern} if pattern else None
        for key, value in self._scan_match(self._store.items(), patterns=patterns, limit=limit):
            yield key

    def values(self, patterns: dict = None, limit: int = None):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {VALUE: patterns}
        for _, value in self._scan_match(self._store.items(), patterns, limit):
            yield value

    def items(self, patterns: dict = None, limit: int = None):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {KEY: patterns}
        for key, value in self._scan_match(self._store.items(), patterns, limit):
            yield key, value

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

    def delete(self, key):
        return self.pop(key)

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

    def keys(self, pattern: str = None, limit: int = None, **kwargs):
        patterns = {KEY: pattern} if pattern else None
        with self.manager.open(self.store_path, **self.open_params) as store:
            for key, _ in self._scan_match(store.items(), patterns=patterns, limit=limit):
                yield key

    def values(self, patterns: str = None, limit: int = None, **kwargs):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {VALUE: patterns}
        with self.manager.open(self.store_path, **self.open_params) as store:
            for item in self._scan_match(store.items(), patterns=patterns, limit=limit):
                yield item[1]

    def items(self, patterns: str = None, limit: int = None, **kwargs):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {KEY: patterns}
        with self.manager.open(self.store_path, **self.open_params) as store:
            for item in self._scan_match(store.items(), patterns=patterns, limit=limit):
                yield item

    def get(self, key, default=None):
        with self.manager.open(self.store_path, **self.open_params) as store:
            key = self.encode_key(key)
            if key in store:
                return self.decode_value(store[key])
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
            for key in keys:
                yield self.decode_value(store.get(self.encode_key(key), default))

    def set_batch(self, keys, values):
        with self.manager.open(self.store_path, **self.open_params) as store:
            for key, value in zip(keys, values):
                store[self.encode_key(key)] = self.encode_value(value)
        return True

    def _cp(self, source, target):
        source_path = self._get_path(source)
        target_path = self._get_path(target)
        target_path.write_bytes(source_path.read_bytes())

    def save(self, path):
        self._cp(self.store_path, path)
        return path

    def load(self, path):
        self._cp(path, self.store_path)
        return self

    scan = items


from .inmemory import InMemoryStore
from .shelve import ShelveStore

with contextlib.suppress(ImportError):
    from .redis import RedisStore

with contextlib.suppress(ImportError):
    from .lmdb import LmdbStore

with contextlib.suppress(ImportError):
    from .pysos import PysosStore

with contextlib.suppress(ImportError):
    from .dynamodb import DynamoDBStore

with contextlib.suppress(ImportError):
    from .firestore import Firestore

with contextlib.suppress(ImportError):
    from .buckets import BucketStore

with contextlib.suppress(ImportError):
    from .cosmos import CosmosDBStore

with contextlib.suppress(ImportError):
    from .mongodb import MongoDBStore
