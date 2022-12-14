import lmdbm
import cloudpickle
from lmdbm.lmdbm import remove_lmdbm

from spoonbill.stores import ContextStore


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


class LmdbDict(ContextStore):
    manager = CloudpickleEncoder

    def __init__(self, path: str, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True,
                 strict=False):
        self.store_path = path
        self.strict = strict
        self.open_params = {"flag": flag, "mode": mode, "map_size": map_size, "autogrow": autogrow}

    def _flush(self):
        remove_lmdbm(self.store_path)

    @property
    def map_size(self):
        return self.open_params.get("map_size")

    @property
    def autogrow(self):
        return self.open_params.get("autogrow")

    @classmethod
    def from_db(cls, db_path, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True,
                strict=False):
        return LmdbDict(
            path=db_path,
            flag=flag,
            mode=mode,
            map_size=map_size,
            autogrow=autogrow,
            strict=strict
        )

    open = from_db
