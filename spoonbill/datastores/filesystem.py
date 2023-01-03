from spoonbill.datastores import KeyValueStore, InMemoryStore
from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.cached import SimpleCacheFileSystem
import typing
import fsspec

import cloudpickle


class FilesystemStore(KeyValueStore):
    """
    A dictionary implemented as map from file name as key to file contents as value.
    Pros: cloud persistent, cheap.
    Cons: slow.
    """

    def __init__(self, path, **kwargs):
        self._store = fsspec.get_mapper(path, **kwargs)
        self.store_path = path
        self.strict = False
        self.as_string = False

    def encode_value(self, value):
        return cloudpickle.dumps(value)

    def decode_value(self, value):
        return cloudpickle.loads(value)

    def encode_key(self, key):
        return key

    def decode_key(self, key):
        return key

    @classmethod
    def open(self, path: str, **kwargs):
        return FilesystemStore(path, **kwargs)

    def _flush(self):
        for key in self.keys():
            del self._store[key]

    def set(self, key, value):
        self[key] = value
        return True

    def values(self, keys=None):
        if keys is None:
            keys = self.keys()
        for key in keys:
            yield self[key]

    def popitem(self):
        item = self._store.popitem()
        return self.decode_value(item[1])

    def save(self, path, **kwargs):
        target_path = fsspec.get_mapper(path, **kwargs)
        for key, value in self.items():
            target_path[self.encode(key)] = self.encode(value)
        return True

    def load(self, path, **kwargs):
        fs = spoonbill.filesystem.get_filesystem_from_path(path, **kwargs)
        if fs.isfile(path):
            store = InMemoryStore.load(self, path)
        elif fs.isdir(path):
            store = fsspec.get_mapper(path, **kwargs)
        else:
            raise RuntimeError(f"Path {path} is not a file or directory")
        self._flush()
        for key, value in store.items():
            self[key] = value
        return self
