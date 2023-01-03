import typing
import contextlib
import cloudpickle
import re
from spoonbill.filesystem import FileSystem

KEY = 'ID__'
VALUE = 'VALUE__'
RANDOM_VALUE = '#5f1a7da3a2b04d629231108bb6548dcb#'


class Strict:
    """
    A class to wrap a dict-like object to make it strict or not.
    A non-strict dict will encode all keys and values to bytes using cloudpickle.
    Pros - Very flexiable and can be use in the same way as a normal dict, everywhere.
    Con - On some backends, a loss of functionality and speed is possible.
    """
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

    def save(self, path, **kwargs):
        FileSystem(path, **kwargs).write_bytes(cloudpickle.dumps(self))
        return True

    @classmethod
    def load(cls, path, **kwargs):
        return cloudpickle.loads(FileSystem(path).read_bytes())


class KeyValueStore(Strict):
    """
    A base class for a key value store.
    """
    _store = None
    strict: bool = False

    def __init__(self, store, strict: bool = False):

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

    def _to_key_value(self, item):
        if item:
            return self.decode_key(item[0]), self.decode_value(item[1])

    def _to_filter(self, feature, pattern):
        if isinstance(pattern, str):
            re_pattern = re.compile(pattern)
            return lambda x: re_pattern.match(str(x.get(feature) if isinstance(x, dict) else x))
        return lambda x: x.get(feature) == pattern if isinstance(x, dict) else x == pattern

    def _scan_match(self, sequence: typing.Sequence, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        conditions = conditions or {}
        filters = []
        for feature, pattern in conditions.items():
            filters.append(self._to_filter(feature, pattern))

        for i, item in enumerate(sequence):
            if i == limit:
                break
            key, value = self._to_key_value(item)
            if not conditions:
                yield key, value
            else:
                if isinstance(value, dict) and VALUE in value:
                    value = value[VALUE]
                if all([validate(value) for validate in filters]):
                    yield key, value

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        is_valid = self._to_filter(KEY, pattern) if pattern else lambda x: True
        for i, key in enumerate(self._store.keys()):
            if i == limit:
                break
            key = self.decode_key(key)
            if is_valid(key):
                yield key

    def values(self, keys: list = None, limit: int = None, default=None):
        if keys:
            for i, key in enumerate(keys):
                if i == limit:
                    break
                yield self.get(key, default)
        else:
            for i, value in enumerate(self._store.values()):
                if i == limit:
                    break
                yield value

    def items(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        for key, value in self._scan_match(self._store.items(), conditions, limit):
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

    def __repr__(self):
        size = len(self)
        items = str({key: value for i, (key, value) in enumerate(self.items()) if i < 5})[
                :-1] + '...' if size > 5 else str({key: value for key, value in self.items()})
        return f"{self.__class__.__name__}() of size {size}\n{items}"

    def _from_directory(self, path):
        mapper = FileSystem(path).get_mapper()
        self._flush()

        def decode_key(key):
            if key is not None:
                if isinstance(key, str) and str(key)[:3] in ('b"\\', "b'\\"):
                    return cloudpickle.loads(eval(key))
                return key

        for key, value in mapper.items():
            self[decode_key(key)] = cloudpickle.loads(value)
        return self

    def reload(self, other):
        if isinstance(other, str):
            filesystem = FileSystem(other)
            if filesystem.fs.isfile(other):
                store = cloudpickle.loads(filesystem.read_bytes(other))
            elif filesystem.fs.isdir(other):
                return self._from_directory(other)
            else:
                raise ValueError(f"Path {other} is not a file or directory")
        elif isinstance(other, KeyValueStore):
            store = other
        self._flush()
        for key, value in store.items():
            self[key] = value
        return self


class ContextStore(KeyValueStore, Strict):
    """
    The base class for context store - where the backend is used as a context manager.
    """
    context = None

    def __getitem__(self, item):
        with self.context as store:
            return self.decode_value(store[self.encode_key(item)])

    def __setitem__(self, key, value):
        with self.context as store:
            store[self.encode_key(key)] = self.encode_value(value)

    def __contains__(self, item):
        with self.context as store:
            return self.encode_key(item) in store

    def __len__(self):
        with self.context as store:
            return len(store)

    def keys(self, pattern: str = None, limit: int = None, **kwargs):
        is_valid = self._to_filter(KEY, pattern) if pattern else lambda x: True
        with self.context as store:
            for key in store.keys():
                key = self.decode_key(key)
                if is_valid(key):
                    yield key

    def _iter_items(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        with self.context as store:
            for item in self._scan_match(store.items(), conditions=conditions, limit=limit):
                yield item

    def values(self, keys: list = None, limit: int = None, default=None):
        if keys:
            with self.context as store:
                for key in keys:
                    value = store.get(self.encode_key(key))
                    if value is None:
                        yield default
                    else:
                        yield self.decode_value(value)
        else:
            for item in self._iter_items(limit=limit):
                yield item[1]

    def items(self, conditions: str = None, limit: int = None, **kwargs):
        for item in self._iter_items(conditions, limit):
            yield item

    def get(self, key, default=None):
        with self.context as store:
            key = self.encode_key(key)
            if key in store:
                return self.decode_value(store[key])
        return default

    def set(self, key, value):
        with self.context as store:
            store[self.encode_key(key)] = self.encode_value(value)
        return True

    def pop(self, key, default=None):
        with self.context as store:
            return self.decode_value(store.pop(self.encode_key(key), default))
        return default

    def popitem(self):
        with self.context as store:
            return self.encode_value(store.popitem())

    def update(self, d):
        with self.context as store:
            store.update({self.encode_key(key): self.encode_value(value) for key, value in d.items()})
        return self

    def _cp(self, source, target, **kwargs):

        FileSystem(target).write_bytes(FileSystem(source, **kwargs).read_bytes())

        # save_bytes(target, load_bytes(source, **kwargs), **kwargs)
        return True

    def save(self, path):
        self._cp(self.store_path, path)
        return path

    def load(self, path):
        self._cp(path, self.store_path)
        return self


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
    from .filesystem import FilesystemStore

with contextlib.suppress(ImportError):
    from .cosmos import CosmosDBStore

with contextlib.suppress(ImportError):
    from .mongodb import MongoDBStore

with contextlib.suppress(ImportError):
    from .safetensors import SafetensorsStore, SafetensorsInMemoryStore, serialize, deserialize

with contextlib.suppress(ImportError):
    from .safetensors import SafetensorsLmdbStore
