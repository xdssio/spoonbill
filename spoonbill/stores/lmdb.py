import contextlib
import json
import pathlib
import os
import lmdbm
import cloudpickle
from spoonbill.stores.base import KeyValueBase
import shutil


class CloudpickleEncoder(lmdbm.Lmdb):
    @staticmethod
    def encode(value):
        return cloudpickle.dumps(value)

    @staticmethod
    def decode(value):
        if value:
            return cloudpickle.loads(value)

    def _pre_key(self, value):
        return self.encode(value)

    def _post_key(self, value):
        return self.decode(value)

    def _pre_value(self, value):
        return self.encode(value)

    def _post_value(self, value):
        return self.decode(value)


class LmdbDict(KeyValueBase):

    def __init__(self, store, db_path):
        self._store = store
        self.db_path = db_path

    @property
    def map_size(self):
        return self._store.map_size

    @property
    def autogrow(self):
        return self._store.autogrow

    def set(self, key, value):
        self._store[key] = value
        return True

    def items(self):
        for key in self.keys():
            yield key, self[key]

    def _flush(self):
        map_size, autogrow = self.map_size, self.autogrow
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.db_path)
        self._store = CloudpickleEncoder.open(self.db_path, flag="c", mode=0o755, map_size=map_size, autogrow=autogrow)
        return True

    @classmethod
    def from_db(cls, db_path, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True):
        return LmdbDict(
            store=CloudpickleEncoder.open(db_path, flag=flag, mode=mode, map_size=map_size, autogrow=autogrow),
            db_path=db_path)

    def set_batch(self, keys, values):
        self.update(zip(keys, values))
        return True

    open = from_db
