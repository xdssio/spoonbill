from spoonbill.datastores.base import KeyValueStore, VALUE, KEY
import modal
import re

name_pattern = re.compile(r"[^a-zA-Z\d\s:]|[ ]")

name_pattern.sub('_', "fdsaf asf _asfsa")


class ModalStore(KeyValueStore):
    KEYS = 'keys'
    SIZE = 'size'

    def __init__(self, name: str, stub: modal.Stub = None, app: modal.App = None, data: dict = {}, **kwargs):
        self.name = name
        self._name = name_pattern.sub('_', self.name)
        self._app = modal.container_app
        if stub is not None:
            setattr(stub, self._data_name, modal.Dict(data=data))
            setattr(stub, self._metadata_name,
                    modal.Dict(data={ModalStore.KEYS: set(data.keys()), ModalStore.SIZE: len(data)}))
        if app is not None:
            self._app = app

    @property
    def _data_name(self):
        return self._name.replace('-', '_') + '_data'

    @property
    def _metadata_name(self):
        return self._name + '_metadata'

    @classmethod
    def open(cls, name: str, stub: modal.Stub = None, app: modal.App = None, data: dict = {}, **kwargs):
        return ModalStore(name=name, stub=stub, app=app, data=data, **kwargs)

    def _contains(self, key):
        """
        Check if the key is in the modal dict.
        :param key:
        :return:
        """
        return key in self._data

    @property
    def _keys(self):
        return self._metadata[ModalStore.KEYS]

    @property
    def _data(self):
        return getattr(self._app, self._data_name)

    @property
    def _metadata(self):
        return getattr(self._app, self._metadata_name)

    def _iter_keys(self):
        for key in self._keys:
            if self._contains(key):
                yield key

    def _remove_key(self, key):
        self._metadata.put(ModalStore.KEYS, self._keys.difference([key]))

    def set_context(self, app):
        self._app = app

    def _setitem(self, key, value):
        if not self._contains(key):
            self._metadata.put(ModalStore.KEYS, self._keys.union([key]))
        self._data.put(key, value)

    def __setitem__(self, key, value):
        self._setitem(key, value)

    def set(self, key, value):
        self._setitem(key, value)

    def _getitem(self, key, default=None):
        if self._contains(key):
            return self._data.get(key)
        return default

    def __getitem__(self, key):
        value = self._getitem(key)
        if value is None:
            raise KeyError(key)
        return value

    def get(self, key, default=None):
        return self._getitem(key, default)

    def _delitem(self, key):
        self._remove_key(key)
        return self._data.pop(key)

    def __delitem__(self, key):
        self._delitem(key)

    def __len__(self):
        return self._data.len()

    def keys(self, pattern: str = None, limit: int = None, *args, **kwargs):
        is_valid = self._to_filter(KEY, pattern) if pattern else lambda x: True
        for i, key in enumerate(self._iter_keys()):
            if i == limit:
                break
            if is_valid(key):
                yield key

    def _items(self):
        for key in self._keys:
            if key not in self._data:
                continue
            yield key, self._data.get(key)

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
        data_keys = set(data.keys())
        new_keys = data_keys.difference(self._keys)
        self._data.update(**data)
        self._metadata.put(ModalStore.KEYS, self._keys.union(new_keys))
        return self

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

    def popitem(self):
        if not self._keys:
            raise KeyError('popitem(): dictionary is empty')
        return self.pop(self._keys[0])
