from spoonbill.datastores import KeyValueStore
from rocksdict import Rdict, Options

class RocksdbStore(KeyValueStore):

    @classmethod
    def open(cls, path:str, strict=True,*args,**kwargs):
        store = Rdict(path, options=Options(raw_mode=not strict))
        return RocksdbStore(store, strict=strict, *args,**kwargs)

    def __len__(self):
        self._store
