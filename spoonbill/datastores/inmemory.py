import json
import pathlib
import re
from typing import Sequence

import cloudpickle
from spoonbill.datastores import KeyValueStore


class InMemoryDict(KeyValueStore):
    """
    A key-value store that datastores everything in memory.
    Practically a python dictionary

    """

    def __init__(self, store: dict = None, strict: bool = True):
        """
        :param store: a dictionary to use as the store
        :param strict: if False, encode and decode keys and values with cloudpickle
        """
        self._store = {}
        if store:
            if isinstance(store, dict):
                self._store = store
            elif isinstance(store, str):
                self.load(store)
        self.strict = strict
        self.as_string = False

    @classmethod
    def from_dict(cls, d: dict):
        return InMemoryDict(d)

    @classmethod
    def from_json(cls, j):
        return InMemoryDict(json.loads(j))

    def set(self, key, value):
        self._store[self.encode_key(key)] = self.encode_value(value)
        return True

    def _flush(self):
        ret = len(self)
        self._store = {}
        return ret

    @classmethod
    def open(self, path=None, strict=True, *args, **kwargs):
        """
        This is a dummy method to make the API consistent with other datastores
        :param args:
        :param kwargs:
        :return:
        """
        return InMemoryDict(store=path, strict=strict)

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        for key in self._to_iter(self._store.keys(), pattern, limit):
            yield self.decode_key(key)

    def values(self, limit: int = None):
        for value in self._store.values():
            if value is not None:
                yield self.decode_value(value)

    def items(self, pattern: str = None, limit: int = None):
        for key, value in self._to_iter(self._store.items(), pattern, limit):
            if value is not None:
                yield self.decode_key(key), self.decode_value(value)

    scan = items

    def save(self, path):
        target_path = self._get_path(path)
        target_path.write_bytes(cloudpickle.dumps(self))
        return path

    def load(self, path):
        path = self._get_path(path)
        loaded = cloudpickle.loads(path.read_bytes())
        self._store = loaded._store
        self.strict = loaded.strict
        return self

    @staticmethod
    def _is_encoded(value):
        return str(value)[:2] == "b'"
