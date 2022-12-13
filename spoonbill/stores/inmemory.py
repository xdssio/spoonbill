import json
import re
from spoonbill.stores.base import KeyValueBase


class InMemoryDict(KeyValueBase):
    """
    A key-value store that stores everything in memory.
    Practically a python dictionary
    """

    def __init__(self, store: dict = None):
        self._store = store or {}

    @classmethod
    def from_dict(cls, d: dict):
        return InMemoryDict(d)

    @classmethod
    def from_json(cls, j):
        return InMemoryDict(json.loads(j))

    def set(self, key, value):
        self._store[key] = value
        return True

    def _flush(self):
        self._store = {}
        return True

    def update(self, d):
        self._store.update(d)
        return self

    def items(self):
        return self._store.items()

    def keys(self, pattern: str = None, *args, **kwargs):
        if pattern:
            pattern = re.compile(pattern)
            return {key for key in self._store.keys() if pattern.match(str(key))}
        return iter(self._store.keys())

    @classmethod
    def open(self, *args, **kwargs):
        """
        This is a dummy method to make the API consistent with other stores
        :param args:
        :param kwargs:
        :return:
        """
        return InMemoryDict()