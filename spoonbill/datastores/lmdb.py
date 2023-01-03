import contextlib

import lmdbm
import cloudpickle
from lmdbm.lmdbm import remove_lmdbm
from spoonbill.datastores import ContextStore
from spoonbill.filesystem import FileSystem


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


class LmdbStore(ContextStore):
    """
    An LMDB key-value store based on [lmdb-python-dbm](https://github.com/Dobatymo/lmdb-python-dbm).

    This is ideal for lists or datastores which either need persistence, are too big to fit in memory or both.
    This is a Python DBM interface style wrapper around [LMDB](http://www.lmdb.tech/doc/) (Lightning Memory-Mapped Database).
    It uses the existing lower level Python bindings [py-lmdb](https://lmdb.readthedocs.io/en/release/).
    This is especially useful on Windows, where otherwise dbm.dumb is the default dbm database.

    """
    manager = CloudpickleEncoder

    def __init__(self, path: str, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True,
                 strict=True):
        self.store_path = path
        self.strict = strict
        self.as_string = False
        self.open_params = {"flag": flag, "mode": mode, "map_size": map_size, "autogrow": autogrow}

    @property
    def context(self):
        return CloudpickleEncoder.open(self.store_path, **self.open_params)

    def _flush(self):
        count = len(self)
        remove_lmdbm(self.store_path)
        return count

    @property
    def map_size(self):
        return self.open_params.get("map_size")

    @property
    def autogrow(self):
        return self.open_params.get("autogrow")

    @classmethod
    def open(cls, db_path, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True,
             strict=True):
        return LmdbStore(
            path=db_path,
            flag=flag,
            mode=mode,
            map_size=map_size,
            autogrow=autogrow,
            strict=strict
        )

    def save(self, path):
        FileSystem(self.store_path).copy_dir(self.store_path, path)
        return True

    def load(self, path):
        FileSystem(path).copy_dir(path, self.store_path)
        return self
