import contextlib
from spoonbill.datastores import KeyValueStore, VALUE
import modal


class ModalStore(KeyValueStore):
    KEYS = 'keys'
    SIZE = 'size'

    def __init__(self, stub: str, data={}):
        self.stub = stub
        self.stub.main = modal.Dict(data=data)
        self.stub.metadata = modal.Dict(data={ModalStore.KEYS: set(data.keys()), ModalStore.SIZE: len(data)})
        self._store = None

    @property
    def _keys(self):
        return self._store.metadata[ModalStore.KEYS]

    @property
    def _size(self):
        return self._store.metadata[ModalStore.SIZE]

    def _add_key(self, key):
        if key not in self._store.main:
            self._increment_size()
        self._store.metadata.put(ModalStore.KEYS, self._keys.union([key]))

    def _remove_key(self, key):
        if key in self._keys:
            self._decrement_size()
        self._store.metadata.put(ModalStore.KEYS, self._keys.difference([key]))

    def _increment_size(self):
        self._store.metadata.put(ModalStore.SIZE, self._size + 1)

    def _decrement_size(self):
        self._store.metadata.put(ModalStore.SIZE, self._size - 1)

    def keys(self):
        return iter(self._store._keys)

    def set_store(self, store):
        self._store = store

    def _setitem(self, key, value):
        self._store.main[key] = value
        self._add_key(key)

    def __setitem__(self, key, value):
        self._setitem(key, value)

    def set(self, key, value):
        self._setitem(key, value)

    def _getitem(self, key, default=None):
        if key in self._store.keys[ModalStore.KEYS]:
            return self._store.main[key]
        return default

    def __getitem__(self, key):
        value = self._getitem(key)
        if value is None:
            raise KeyError(key)
        return value

    def get(self, key, default=None):
        return self._getitem(key, default)

    def _delitem(self, key):
        self._store.main.pop(key)
        self._remove_key(key)

    def __delitem__(self, key):
        self._delitem(key)

    def __len__(self):
        return self._size

    def keys(self):
        for key in self._keys:
            if key not in self._store.main:
                continue
            yield key

    def _items(self):
        for key in self._keys:
            if key not in self._store.main:
                continue
            yield key, self._store.main[key]

    def items(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        for key, value in self._scan_match(self._items(), conditions, limit):
            yield key, value

    def values(self, keys: list = None, limit: int = None, default=None):
        if keys:
            for i, key in enumerate(keys):
                if i == limit:
                    break
                yield self.get(key, default)
        else:
            for i, key in enumerate(self._keys):
                if i == limit:
                    break
                yield self._store.main[key]

    def encode_key(self, key):
        return key

    def encode_value(self, value):
        return value

    def decode_key(self, key):
        return key

    def decode_value(self, value):
        return value
