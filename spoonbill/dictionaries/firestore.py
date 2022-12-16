from spoonbill.dictionaries import KeyValueStore, KEY, VALUE
from google.cloud import firestore

RANDOM_VALUE = '#5f1a7da3a2b04d629231108bb6548dcb#'


class FireStoreDict(KeyValueStore):

    def __init__(self, collection_name: str, strict: bool = False, **kwargs):
        self.collection_name = collection_name
        self.strict = strict
        self.client = firestore.Client(**kwargs)
        self.collection = self.client.collection(collection_name)
        self.strict = strict

    @classmethod
    def open(self, collection_name: str, strict: bool = False, **kwargs):
        return FireStoreDict(collection_name=collection_name, strict=strict, **kwargs)

    def _flush(self):
        i = -1
        for i, doc in enumerate(self.collection.stream()):
            doc.reference.delete()
        return i + 1

    def _list_tables(self):
        return list(self.client.collections())

    def _get_item(self, key: str):
        ref = self.collection.document(self.encode_key(key)).get()
        if ref.exists:
            return self.decode_value(ref.to_dict().get(VALUE))
        return None

    def _put_item(self, key: str, value: str):
        ref = self.collection.document(self.encode_key(key))
        ref.set({VALUE: self.encode_value(value)})

    def _delete_item(self, key: str):
        self.collection.document(self.encode_key(key)).delete()

    def __len__(self):
        i = -1
        for i, value in enumerate(self.collection.stream()): pass
        return i + 1

    def __getitem__(self, item):
        ret = self._get_item(item)
        if ret is None:
            raise KeyError(item)
        return ret

    def __setitem__(self, key, value):
        return self._put_item(key, value)

    def __delitem__(self, key):
        self._delete_item(key)

    def get(self, key, default=None):
        item = self._get_item(key)
        if item is None:
            item = default
        return item

    def set(self, key, value):
        return self._put_item(key, value)

    def keys(self, pattern: str = None, count: int = None):
        def _scan():
            for doc in self.collection.stream():
                yield self.decode_key(doc.id)

        for item in self._to_iter(_scan(), pattern=pattern, count=count):
            yield item

    def values(self, limit: int = None):
        def _scan():
            for doc in self.collection.stream():
                value = doc.to_dict().get(VALUE)
                if value:
                    yield self.decode_value(value)

        for item in self._to_iter(_scan()):
            yield item

    def items(self, pattern: str = None, count: int = None):
        def _scan():
            for doc in self.collection.stream():
                value = doc.to_dict().get(VALUE)
                if value:
                    yield self.decode_key(doc.id), self.decode_value(value)

        for item in self._to_iter(_scan(), pattern=pattern, count=count):
            yield item

    def pop(self, key, default=None):
        value = self._get_item(key)
        if value:
            self._delete_item(key)
            return value
        return default

    def popitem(self):
        for key, value in self.items(limit=1):
            self._delete_item(key)
            return key, value
        raise KeyError('popitem(): dictionary is empty')

    def _update(self, items):
        batch = self.client.batch()
        for i, (key, value) in enumerate(items):
            ref = self.collection.document(self.encode_key(key))
            batch.set(ref, {VALUE: self.encode_value(value)})
        batch.commit()
        return i

    def update(self, d):
        """
       Insert a batch of items into the table in chunks of 25 items.
       :param d: A dictionary like object with items() method
       :return: Count of items inserted
       """
        return self._update(d.items())

    def set_batch(self, keys, values):
        """
         Insert a batch of items into the table in chunks of 25 items.
         :param d: A dictionary like object with items() method
         :return: Count of items inserted
         """
        return self._update(zip(keys, values))

    def get_batch(self, keys, default=None):
        for key in keys:
            yield self.get(key, default)
