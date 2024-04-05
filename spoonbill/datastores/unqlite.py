import contextlib
import json
import cloudpickle

from spoonbill.datastores.base import KeyValueStore
from unqlite import UnQLite


class UnQLiteStore(KeyValueStore):
    """
    A simple dictionary implementation.
    Pros: fast, cheap.
    Cons: Not persistent.

    """

    def __init__(self, store: UnQLite = None, strict: bool = True):
        """
        :param store: a dictionary to use as the store
        :param strict: if False, encode and decode keys and values with cloudpickle
        """
        if store is None:
            store = UnQLite()
        self._store = store
        self.strict = strict
        self.as_string = not strict

    @classmethod
    def from_dict(cls, d: dict, strict=True):
        store = UnQLiteStore(strict=strict)
        store.update(d)
        return store

    @classmethod
    def from_json(cls, j):
        return UnQLiteStore.from_dict(json.loads(j))

    def set(self, key, value):
        self._store[self.encode_key(key)] = self.encode_value(value)
        return True

    def get(self, key, default=None):
        ret = default
        with contextlib.suppress(KeyError):
            ret = self.decode_value(self._store[self.encode_key(key)])
        return ret

    def _flush(self):
        ret = len(self)
        self._store = {}
        return ret

    @classmethod
    def open(cls, path: str = None, strict=True, *args, **kwargs):
        """
        This is a dummy method to make the API consistent with other datastores
        :param args:
        :param kwargs:
        :return:
        """
        store = UnQLite(path) if path else UnQLite()
        return UnQLiteStore(store=store, strict=strict)

    def pop(self, key, default=None):
        ret = self.get(key, default)
        del self._store[self.encode_key(key)]
        return ret

    def update(self, d):
        with self._store.transaction():
            for key, value in d.items():
                self._store[self.encode_key(key)] = self.encode_value(value)
        return self

    @classmethod
    def load(cls, path, **kwargs):
        return cls.open(path=path, **kwargs)

    def encode(self, value):
        """Encode a value to a string.
        Note were using the str instead of encoding to bytes because it works best with
        numbers too without creating issues with different encodings.
        """
        encoded = cloudpickle.dumps(value)
        return str(encoded)

    def decode(self, value):
        if value is not None:
            if self._is_encoded(value):
                value = eval(value)
                value = cloudpickle.loads(value)
                return value
            return value

    def _is_encoded(self, value):
        return str(value)[:3] in ('b"\\', "b'\\", 'b"b', "b'b", 'b"b', "b'b")
