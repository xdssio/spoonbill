from spoonbill.datastores.base import KeyValueStore, KEY, VALUE
from typing import List
import plyvel


class LevelDBStore(KeyValueStore):

    NONE = '__NONE__'

    @classmethod
    def open(self, path: str, strict: bool = True, *args, **kwargs):
        db = plyvel.DB(path, create_if_missing=True)
        return LevelDBStore(db, strict=strict, *args, **kwargs)

    def __init__(self, store, strict=False, name: str = None, options=None):
        super().__init__(store, strict)
        self.options = options or {}
        self._size = self._count_all()
        self.name = name
        self.as_string = False

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        is_valid = self._to_filter(KEY, pattern) if pattern else lambda x: True
        for i, (key, _) in enumerate(self._store):
            if i == limit:
                break
            key = self.decode_key(key)
            if is_valid(key):
                yield key

    def _count_all(self):
        count = 0
        for _ in self._store:
            count += 1
        return count

    def __setitem__(self, key, value):
        if value is None:
            value = LevelDBStore.NONE
        key = self.encode_key(key)
        value = self.encode_value(value)
        if key not in self._store:
            self._size += 1
        return self._store.put(key, value)

    def __getitem__(self, item):
        return self.decode_value(self._store.get(self.encode_key(item)))

    def decode_value(self, value):

        if self.strict:
            if value == LevelDBStore.NONE:
                return None
            return value
        decoded = self.decode(value)
        if decoded == LevelDBStore.NONE:
            return None
        return decoded

    def set(self, key, value):
        self[key] = value

    def delete(self, key):
        return self.pop(key, None)

    def __contains__(self, item):
        return self._store.get(self.encode_key(item)) is not None

    def pop(self, key, default=None):
        value = default
        if key in self:
            self._size -= 1
            value = self.get(key)
        key = self.encode_key(key)
        self._store.delete(key)
        return value

    def set(self, key, value):
        self[key] = value

    def popitem(self):
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        key = next(self.keys())
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

    def items(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        for key, value in self._scan_match(self._store, conditions, limit):
            yield key, value

    def values(self, keys: list = None, limit: int = None, default=None):
        if keys:
            for i, key in enumerate(keys):
                if i == limit:
                    break
                yield self.get(key, default)
        else:
            for i, (_, value) in enumerate(self._store):
                if i == limit:
                    break
                yield self.decode_value(value)

    def update(self, d: dict):
        wb = self._store.write_batch()
        count = 0
        for key, value in d.items():
            if key not in self:
                count += 1
            wb.put(self.encode_key(key), self.encode_value(value))
        wb.write()
        self._size += count
        return self

    def _flush(self):
        wb = self._store.write_batch()
        for key, _ in self._store:
            wb.delete(key)
        wb.write()
        self._size = 0

    def __len__(self):
        return self._size
