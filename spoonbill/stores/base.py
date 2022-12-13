import json
import typing
import contextlib
import warnings

import cloudpickle
import re


def to_int(i):
    with contextlib.suppress(ValueError):
        return int(i)
    return False


def to_float(f):
    with contextlib.suppress(ValueError):
        return float(f)
    return False


class KeyValueBase:
    _store: typing.Any = None
    as_strings: bool = False

    def __init__(self, store: typing.Any, string_keys: bool = False):
        self._store = store
        self.as_strings = string_keys

    def __getitem__(self, item):
        return self._store[item]

    def __setitem__(self, key, value):
        self._store[key] = value

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

    def keys(self, *args, **kwargs):
        return self._store.keys(*args, **kwargs)

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

    def get_batch(self, keys, default=None):
        for key in keys:
            yield self.get(key, default)

    def set_batch(self, keys, values):
        for key, value in zip(keys, values):
            self.set(key, value)

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

    @staticmethod
    def encode(value):
        return str(cloudpickle.dumps(value))

    @staticmethod
    def decode(value):
        if value is not None:
            return cloudpickle.loads(eval(value))

    def __repr__(self):
        size = len(self)
        items = str({key: value for i, (key, value) in enumerate(self.items()) if i < 5})[
                :-1] + '...' if size > 5 else str({key: value for key, value in self.items()})
        return f"{self.__class__.__name__}() of size {size}\n{items}"


class ContextStoreBase(KeyValueBase):
    store_path: str = None
    manager: typing.Any = None

    def __getitem__(self, item):
        with self.manager.open(self.store_path) as store:
            return store[item]

    def __setitem__(self, key, value):
        with self.manager.open(self.store_path) as store:
            store[key] = value

    def __contains__(self, item):
        with self.manager.open(self.store_path) as store:
            return item in store

    def __len__(self):
        with self.manager.open(self.store_path) as store:
            return len(store)

    def keys(self, *args, **kwargs):
        with self.manager.open(self.store_path) as store:
            for key in store.keys(*args, **kwargs):
                yield key

    def values(self, *args, **kwargs):
        with self.manager.open(self.store_path) as store:
            for value in store.values(*args, **kwargs):
                yield value

    def items(self, *args, **kwargs):
        with self.manager.open(self.store_path) as store:
            for item in store.items(*args, **kwargs):
                yield item

    def get(self, key, default=None):
        with self.manager.open(self.store_path) as store:
            if key in store:
                return store[key]
        return default

    def set(self, key, value):
        with self.manager.open(self.store_path) as store:
            store[key] = value
        return True
