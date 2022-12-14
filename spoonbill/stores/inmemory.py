import json
import pathlib
import re
import cloudpickle
from spoonbill.stores import KeyValueStore


class InMemoryDict(KeyValueStore):
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


