import typing

from spoonbill.datastores import KeyValueStore, VALUE, KEY
from google.cloud import firestore
import re


class Firestore(KeyValueStore):

    def __init__(self, collection_name: str, strict: bool = True, **kwargs):
        self.collection_name = collection_name
        self.strict = strict
        self.client = firestore.Client(**kwargs)
        self.collection = self.client.collection(collection_name)
        self.strict = strict
        self.as_string = True

    @classmethod
    def open(self, collection_name: str, strict: bool = False, **kwargs):
        return Firestore(collection_name=collection_name, strict=strict, **kwargs)

    def _flush(self):
        i = -1
        for i, doc in enumerate(self.collection.stream()):
            doc.reference.delete()
        return i + 1

    def _list_tables(self):
        return list(self.client.collections())

    def _to_item(self, value):
        if self.strict and isinstance(value, dict):
            return value
        return {VALUE: self.encode_value(value)}

    def _from_item(self, item):
        if item is not None:
            if VALUE in item:
                item = item[VALUE]
            return self.decode_value(item)

    def _get_item(self, key: str):
        ref = self.collection.document(self.encode_key(key)).get()
        if ref.exists:
            return self._from_item(ref.to_dict())
        return None

    def _put_item(self, key: str, value: str):
        ref = self.collection.document(self.encode_key(key))
        ref.set(self._to_item(value))

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

    def _scan_query(self, patterns: dict = None, limit: int = None):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {VALUE: patterns}
        elif patterns is None:
            patterns = {}
        query = self.collection
        for feature, value in patterns.items():
            query = query.where(feature, '==', value)
        if limit is not None:
            query = query.limit(limit)
        return query.stream()

    def keys(self, pattern: str = None, limit: int = None):
        pattern = re.compile(pattern) if pattern else None

        def _scan():
            for i, doc in enumerate(self.collection.stream()):
                if i == limit:
                    break
                key = self.decode_key(doc.id)
                if pattern and not pattern.match(key):
                    continue
                else:
                    yield self.decode_key(doc.id)

        for key in _scan():
            yield key

    def values(self, patterns: dict = None, limit: int = None):

        for item in self._scan_query(patterns=patterns, limit=limit):
            yield self._from_item(item.to_dict())

    def items(self, patterns: dict = None, limit: int = None):

        for item in self._scan_query(patterns=patterns, limit=limit):
            yield self.decode_key(item.id), self._from_item(item.to_dict())

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
            batch.set(ref, self._to_item(value))
        batch.commit()
        return i

    def update(self, d):
        """
       Insert a batch of items into the table in chunks of 25 items.
       :param d: A dictionary like object with items() method
       :return: Count of items inserted
       """
        self._update(d.items())
        return self

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
