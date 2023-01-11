import contextlib

from spoonbill.datastores import KeyValueStore
import modal


class ModalStore(KeyValueStore):

    def __init__(self, stub: str, data={}):
        self.stub = stub
        self.stub.main = modal.Dict(data=data)
        self._store = None

    def set_store(self, store):
        self._store = store.main

    def __len__(self):
        return self._store.len()

    def keys(self):
        return iter(self._store)

    def items(self):
        with contextlib.suppress(KeyError):
            for key in self._store:
                yield key, self._store[key]

    def __repr__(self):
        return f"ModalStore('main', {len(self)})"

    def encode_key(self, key):
        return key

    def encode_value(self, value):
        return value

    def decode_key(self, key):
        return key

    def decode_value(self, value):
        return value
