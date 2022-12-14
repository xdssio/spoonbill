import json
import re
from spoonbill.stores.base import KeyValueBase


class InMemoryDict(KeyValueBase):
    """
    A key-value store that stores everything in memory.
    Practically a python dictionary
    """

    def __init__(self, store: dict = None, strict: bool = False):
        self._store = store or {}
        self.strict = strict

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
        self._store = {}
        return True

    @classmethod
    def open(self, *args, **kwargs):
        """
        This is a dummy method to make the API consistent with other stores
        :param args:
        :param kwargs:
        :return:
        """
        return InMemoryDict()
