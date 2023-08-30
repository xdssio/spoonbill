from spoonbill.datastores.rdict import RdictBase
from rocksdict import Rdict, Options, WriteBatch, SstFileWriter


class RocksDBStore(RdictBase):
    """
    A key-value store based on [speedict](https://github.com/speedb-io/speedb)
    """

    @classmethod
    def open(cls, path: str, strict=True, name: str = None, *args, **kwargs):
        store = Rdict(path, options=Options(**kwargs))
        return RocksDBStore(store, strict=strict, name=name)

    @property
    def names(self):
        return Rdict.list_cf(self._store.path())

    def _list_cf(self):
        return

    def _flush(self):
        path = self._store.path()
        self._store.close()
        Rdict.destroy(path)
        self._store = Rdict(path)
        self._size = 0

    def update(self, d: dict):
        wb = WriteBatch()
        count = 0
        for i, (key, value) in enumerate(d.items()):
            wb.put(key, value, self._store.get_column_family_handle('default'))
            if key not in self._store:
                count += 1
        self._store.write(wb)
        self._size += count
        return self

    def save(self, path: str, batch: int = 100000000, **kwargs):
        writer = SstFileWriter(options=Options(**kwargs))
        writer.open(path)
        for k, v in self.items():
            writer[k] = v
        writer.finish()
        return True
