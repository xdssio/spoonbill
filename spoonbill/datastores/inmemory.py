import json
import pathlib
import re
from typing import Sequence

import cloudpickle
from spoonbill.datastores import KeyValueStore, KEY, VALUE


class InMemoryStore(KeyValueStore):
    """
    A simple dictionary implementation.
    Pros: fast, cheap.
    Cons: Not persistent.

    """

    def __init__(self, store: dict = None, strict: bool = True, as_string=False):
        """
        :param store: a dictionary to use as the store
        :param strict: if False, encode and decode keys and values with cloudpickle
        """
        self._store = {}
        if store:
            if isinstance(store, dict):
                self._store = store
            elif isinstance(store, (str, KeyValueStore)):
                self.reload(store)

        self.strict = strict
        self.as_string = as_string

    @classmethod
    def from_dict(cls, d: dict):
        return InMemoryStore(d)

    @classmethod
    def from_json(cls, j):
        return InMemoryStore(json.loads(j))

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
        return InMemoryStore(store=path, strict=strict)

    @staticmethod
    def _is_encoded(value):
        return str(value)[:2] == "b'"
