from spoonbill.datastores import KeyValueStore, VALUE, KEY
import modal


class ModalStore(KeyValueStore):
    KEYS = 'keys'
    SIZE = 'size'

    def __init__(self, stub: str, data={}):
        self.stub = stub
        self.stub._main = modal.Dict(data=data)
        self.stub._metadata = modal.Dict(data={ModalStore.KEYS: set(data.keys()), ModalStore.SIZE: len(data)})
        self._store = None

    def _contains(self, key):
        """
        Check if the key is in the modal dict.
        :param key:
        :return:
        """
        return key in self._store._main

    @property
    def _keys(self):
        return self._store._metadata[ModalStore.KEYS]

    def _iter_keys(self):
        for key in self._keys:
            if self._contains(key):
                yield key

    @property
    def _size(self):
        return self._store._metadata[ModalStore.SIZE]

    def _add_key(self, key):
        if not self._contains(key):
            self._increment_size()
        self._store._metadata.put(ModalStore.KEYS, self._keys.union([key]))

    def _remove_key(self, key):
        if self._contains(key):
            self._decrement_size()
        self._store._metadata.put(ModalStore.KEYS, self._keys.difference([key]))

    def _increment_size(self):
        self._store._metadata.put(ModalStore.SIZE, self._size + 1)

    def _decrement_size(self):
        self._store._metadata.put(ModalStore.SIZE, self._size - 1)

    def set_context(self, app):
        self._store = app

    def _setitem(self, key, value):
        self._add_key(key)
        self._store._main.put(key, value)

    def __setitem__(self, key, value):
        self._setitem(key, value)

    def set(self, key, value):
        self._setitem(key, value)

    def _getitem(self, key, default=None):
        if self._contains(key):
            return self._store._main.get(key)
        return default

    def __getitem__(self, key):
        value = self._getitem(key)
        if value is None:
            raise KeyError(key)
        return value

    def get(self, key, default=None):
        return self._getitem(key, default)

    def _delitem(self, key):
        value = self._store._main.pop(key)
        self._remove_key(key)
        return value

    def __delitem__(self, key):
        self._delitem(key)

    def __len__(self):
        return self._size

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        is_valid = self._to_filter(KEY, pattern) if pattern else lambda x: True
        for i, key in enumerate(self._iter_keys()):
            if i == limit:
                break
            if is_valid(key):
                yield key

    def _items(self):
        for key in self._keys:
            if key not in self._store._main:
                continue
            yield key, self._store._main.get(key)

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
                yield self.get(key, default)

    def update(self, data: dict):
        for key, value in data.items():
            self[key] = value

    def _flush(self):
        for key in self._keys:
            self._delitem(key)

    def encode_key(self, key):
        return key

    def encode_value(self, value):
        return value

    def decode_key(self, key):
        return key

    def decode_value(self, value):
        return value

    def pop(self, key, default=None):
        if not self._contains(key):
            return default
        return self._delitem(key)
