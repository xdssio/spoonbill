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

    def __init__(self, store: dict = None, strict: bool = False):
        """
        :param store: a dictionary to use as the store
        :param strict: if False, encode and decode keys and values with cloudpickle
        """
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
        ret = len(self)
        self._store = {}
        return ret

    @classmethod
    def open(self, path=None, *args, **kwargs):
        """
        This is a dummy method to make the API consistent with other datastores
        :param args:
        :param kwargs:
        :return:
        """
        return InMemoryDict() if path is None else InMemoryDict.from_file(path)

    @staticmethod
    def _is_cloud_url(path):
        if hasattr(path, 'dirname'):
            path = path.dirname
        return re.match("^s3:\/\/|^az:\/\/|^gs:\/\/", path) is not None

    @staticmethod
    def _get_path(path):
        if InMemoryDict._is_cloud_url(path):
            import cloudpathlib
            return cloudpathlib.CloudPath(path)
        return pathlib.Path(path)

    def keys(self, pattern: str = None, count: int = None, *args, **kwargs):
        for key in self._to_iter(self._store.keys(), pattern, count):
            yield self.decode_key(key)

    def values(self, count: int = None):
        for value in self._store.values():
            if value is not None:
                yield self.decode_value(value)

    def items(self, pattern: str = None, count: int = None):
        for key, value in self._to_iter(self._store.items(), pattern, count):
            if value is not None:
                yield self.decode_key(key), self.decode_value(value)

    scan = items
