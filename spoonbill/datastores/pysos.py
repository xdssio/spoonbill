import os
import cloudpickle
import pysos

from spoonbill.datastores import KeyValueStore


class PysosStore(KeyValueStore):
    """
    pySOS: Simple Objects Storage

    This is ideal for lists or datastores which either need persistence, are too big to fit in memory or both.

    There are existing alternatives like shelve, which are very good too. There main difference with pysos is that:

    only the index is kept in memory, not the values (so you can hold more data than what would fit in memory)
    it provides both persistent dicts and lists
    objects must be json "dumpable" (no cyclic references, etc.)
    it's fast (much faster than shelve on windows, but slightly slower than native dbms on linux)
    it's unbuffered by design: when the function returns, you are sure it has been written on disk
    it's safe: even if the machine crashes in the middle of a big write, data will not be corrupted
    it is platform independent, unlike shelve which relies on an underlying dbm implementation, which may vary from system to system
    the data is stored in a plain text format
    """

    def __init__(self, store: pysos.Dict = None, store_path: str = None, strict: bool = True):
        self._store = store or {}
        self.store_path = store_path
        self.strict = strict
        self.as_string = True

    def _flush(self):
        count = len(self)
        os.remove(self.store_path)
        self._store = pysos.Dict(self.store_path)
        return count

    def set(self, key, value):
        self[key] = value
        return True

    def update(self, d):
        for k, v in d.items():
            self[k] = v
        return self

    def pop(self, key, default=None):
        key = self.encode_key(key)
        ret = self._store.get(key, default)
        if key in self._store:
            del self._store[key]
            ret = self.decode_value(ret)
        return ret

    @classmethod
    def open(cls, path: str, strict: bool = True):
        return PysosStore(pysos.Dict(path), path, strict=strict)
