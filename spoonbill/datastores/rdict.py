from spoonbill.datastores.base import KeyValueStore

from typing import List


class RdictBase(KeyValueStore):
    """
    A key-value store based on [speedict](https://github.com/speedb-io/speedb)
    """

    def __init__(self, store, strict=False, name: str = None, options=None):
        super().__init__(store, strict)
        self.options = options or {}
        self._size = self._count_all()
        self.name = name

    def _count_all(self):
        count = 0
        for _ in self._store.keys():
            count += 1
        return count

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

    @classmethod
    def load(cls, path, ssts: List[str], **kwargs):
        raise NotImplementedError(
            'load() is not implemented for SpeedbStore - try using ingest() instead')

    def ingest(self, path: str):
        self._store.ingest_external_file([path])
        self._size = self._count_all()
