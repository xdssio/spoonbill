import typing

from spoonbill.datastores import KeyValueStore, VALUE, KEY
from google.cloud import firestore
import re


class Firestore(KeyValueStore):
    """
    Google cloud firestore key-value store
    """

    def __init__(self, collection_name: str, strict: bool = True, **kwargs):
        self.collection_name = collection_name
        self.strict = strict
        self.client = firestore.Client(**kwargs)
        self.collection = self.client.collection(collection_name)
        self.strict = strict
        self.as_string = True

    @classmethod
    def open(self, collection_name: str, strict: bool = True, **kwargs):
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

    def _to_key_value(self, item):
        if item is not None:
            key = item.id
            value = item.to_dict()
            if VALUE in value:
                value = value[VALUE]
            return self.decode_key(key), self.decode_value(value)

    def _get_item(self, key: str):
        ref = self.collection.document(self.encode_key(key)).get()
        if ref.exists:
            return self._to_key_value(ref)[1]
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

    def _scan_query(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        elif conditions is None:
            conditions = {}
        query = self.collection
        for feature, value in conditions.items():
            query = query.where(feature, '==', value)
        if limit is not None:
            query = query.limit(limit)
        for doc in query.stream():
            yield self._to_key_value(doc)

    def keys(self, pattern: str = None, limit: int = None):
        if pattern is not None and self.strict is False:
            raise ValueError('Cannot use pattern with strict=False mode')
        pattern = re.compile(pattern) if pattern else None
        for i, doc in enumerate(self.collection.stream()):
            if i == limit:
                break
            key = self.decode_key(doc.id)
            if pattern and not pattern.match(key):
                continue
            else:
                yield self.decode_key(doc.id)

    def values(self, keys: list = None, limit: int = None, default=None):
        if keys:
            for key in keys:
                yield self.get(key, default)
        else:
            for item in self.items(limit=limit):
                yield item[1]

    def items(self, conditions: dict = None, limit: int = None):
        if conditions and self.strict is False:
            raise ValueError('Cannot use conditions with strict=False mode')
        for item in self._scan_query(conditions=conditions, limit=limit):
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

    def update(self, d):
        """
       Insert a batch of items into the table in chunks of 25 items.
       :param d: A dictionary like object with items() method
       :return: Count of items inserted
       """
        batch = self.client.batch()
        for i, (key, value) in enumerate(d.items()):
            ref = self.collection.document(self.encode_key(key))
            batch.set(ref, self._to_item(value))
        batch.commit()
        return self
