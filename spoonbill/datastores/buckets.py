from spoonbill.datastores import KeyValueStore, KEY
import fsspec
import cloudpickle


class BucketStore(KeyValueStore):
    """
    A dictionary implemented as a bucket of files.
    Pros: cloud persistent, cheap.
    Cons: slow.
    """

    def __init__(self, path, **kwargs):
        self._store = fsspec.get_mapper(path, **kwargs)
        self.strict = True
        self.as_string = False
        self.path = path

    def encode_value(self, value):
        return cloudpickle.dumps(value)

    def decode_value(self, value):
        return cloudpickle.loads(value)

    def encode_key(self, key):
        return key

    def decode_key(self, key):
        return key

    @classmethod
    def open(self, path: str):
        return BucketStore(path)

    def _flush(self):
        for key in self.keys():
            del self._store[key]

    def set(self, key, value):
        self[key] = value
        return True

    def values(self, keys=None):
        if keys is None:
            keys = self.keys()
        for key in keys:
            yield self[key]

    def popitem(self):
        item = self._store.popitem()
        return self.decode_value(item[1])
