from spoonbill.datastores import KeyValueStore
from speedict import Rdict, Options, WriteBatch, SstFileWriter
from typing import List


class SpeedbStore(KeyValueStore):
    """
    A key-value store based on [speedict](https://github.com/speedb-io/speedb)
    """

    def __init__(self, store, strict=False, options=None):
        super().__init__(store, strict)
        self.options = options or {}
        self._size = self._count_all()

    def _count_all(self):
        count = 0
        for _ in self._store.keys():
            count += 1
        return count

    @classmethod
    def open(cls, path: str, strict=True, *args, **kwargs):
        store = Rdict(path, options=Options(**kwargs))
        return SpeedbStore(store, strict=strict)

    def _list_cf(self):
        return Rdict.list_cf(self._store.path())

    def _flush(self):
        path = self._store.path()
        self._store.close()
        Rdict.destroy(path)
        self._store = Rdict(path)
        self._size = 0

    def __setitem__(self, key, value):
        if key not in self._store:
            self._size += 1
        return KeyValueStore.__setitem__(self, key, value)

    def set(self, key, value):
        if key not in self._store:
            self._size += 1
        return KeyValueStore.set(self, key, value)

    def delete(self, key):
        return self.pop(key, None)

    def pop(self, key, default=None):
        value = default
        if key in self._store:
            self._size -= 1
            value = self.get(key)
        del self._store[key]
        return value

    def set(self, key, value):
        self[key] = value

    def popitem(self):
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        key = next(self._store.keys())
        return self.pop(key)

    def __len__(self):
        return self._size

    def __del__(self):
        self._store.close()

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

    @classmethod
    def load(cls, path, ssts: List[str], **kwargs):
        raise NotImplementedError('load() is not implemented for SpeedbStore - try using ingest() instead')

    def ingest(self, path: str):
        self._store.ingest_external_file([path])
        self._size = self._count_all()
