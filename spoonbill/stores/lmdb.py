import json
import lmdbm
import cloudpickle
from spoonbill.stores.base import KeyValueBase


class LmdbDict(KeyValueBase):

    @classmethod
    def from_db(cls, db_path, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True):
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

        return LmdbDict(CloudpickleEncoder.open(db_path, flag=flag, mode=mode, map_size=map_size, autogrow=autogrow))


class LmdbStringDict(KeyValueBase):

    @classmethod
    def from_db(cls, db_path, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20, autogrow: bool = True):
        class JsonLmdb(lmdbm.Lmdb):
            def _pre_key(self, value):
                return value.encode("utf-8")

            def _post_key(self, value):
                return value.decode("utf-8")

            def _pre_value(self, value):
                return json.dumps(value).encode("utf-8")

            def _post_value(self, value):
                return json.loads(value.decode("utf-8"))

        return LmdbStringDict(JsonLmdb.open(db_path, flag=flag, mode=mode, map_size=map_size, autogrow=autogrow))

d = LmdbDict.from_db("tmp/test.db")
d["test"] = "test"
d
d.scan('t*')
