import re
from spoonbill.datastores import KeyValueStore, VALUE, KEY
import pymongo
import bson

ID = '_id'


class MongoDBStore(KeyValueStore):
    """
    MongoDB wrapper as a key-value store.
    """

    def __init__(self, uri: str = None,
                 database: str = 'db',
                 collection: str = 'collection',
                 index=KEY,
                 strict=True):

        self.client = pymongo.MongoClient(uri) if uri else pymongo.MongoClient()
        self.database = self.client[database]
        self.collection = None
        self.strict = strict
        self.as_string = False
        self.index = index
        self._create_collections(collection)

    def _create_collections(self, collection):
        self.collection = self.database[collection]
        self.collection.create_index(self.index, unique=True)

    @classmethod
    def open(cls, uri: str = None, database: str = 'db', collection: str = 'collection', index=KEY, strict=True):
        return MongoDBStore(uri=uri, database=database, collection=collection, index=index, strict=strict)

    def _list_collections(self):
        return self.database.list_collection_names()

    def _to_item_value(self, value):
        if isinstance(value, dict):
            return value
        return {VALUE: self.encode_value(value)}

    def _to_item(self, key, value):
        key, value = self.encode_key(key), self._to_item_value(value)
        item = {self.index: key, ID: key}
        item.update(value)
        return item

    def _to_key_value(self, item):
        if item is None:
            return None
        key = self.decode_key(item.pop(self.index))
        _ = item.pop(ID, None)
        value = item.get(VALUE) if isinstance(item, dict) and VALUE in item else item
        value = self.decode_value(value)
        return key, value

    def _put_item(self, key, value):
        self.collection.insert_one(self._to_item(key, value))

    def _get_item(self, key):
        return self._to_key_value(self.collection.find_one({ID: self.encode_key(key)}))

    def __len__(self):
        return self.collection.count_documents({})

    def __contains__(self, item):
        return self._get_item(item) is not None

    def __getitem__(self, item):
        value = self._get_item(item)
        if value is None:
            raise KeyError(item)
        return value[1]

    def __setitem__(self, key, value):
        item = self._get_item(key)
        if item is None:
            self._put_item(key, value)
        else:
            item = self._to_item(key, value)
            self.collection.update_one({ID: item.pop(ID)}, {"$set": item})

    def _delete_item(self, key):
        self.collection.delete_one({ID: self.encode_key(key)})

    def __delitem__(self, key):
        self._delete_item(key)

    def get(self, item, default=None):
        item = self._get_item(item)
        if item is None:
            return default
        return item[1]

    def set(self, key, value):
        self._put_item(key, value)

    def _iter(self, conditions: dict = None, limit: int = None):
        params = {}
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        conditions = conditions or {}
        for feature, pattern in conditions.items():
            if isinstance(pattern, str):
                pattern = re.compile(pattern, re.IGNORECASE)
            params[feature] = pattern
        return self.collection.find(**params).limit(limit) if limit else self.collection.find(params)

    def scan(self, pattern: str = None, limit: int = None):
        pattern = {self.index: pattern} if pattern else None
        for item in self._iter(conditions=pattern, limit=limit):
            key, _ = self._to_key_value(item)
            yield key

    def keys(self, pattern: str = None, limit: int = None):
        pattern = {self.index: pattern} if pattern else None
        for item in self._iter(conditions=pattern, limit=limit):
            key, _ = self._to_key_value(item)
            yield key

    def items(self, conditions: dict = None, limit: int = None):
        for item in self._iter(conditions=conditions, limit=limit):
            key, value = self._to_key_value(item)
            yield key, value

    def values(self, keys: dict = None, limit: int = None):
        params = {ID: {'$in': [self.encode_key(key) for key in keys]}} if keys else {}
        for item in self.collection.find(params):
            key, value = self._to_key_value(item)
            yield value

    def update(self, d):
        operations = []
        for key, value in d.items():
            item = self._to_item(key, value)
            operations.append(
                pymongo.ReplaceOne(filter={ID: item.pop(ID)},
                                   replacement=item, upsert=True))
        self.collection.bulk_write(operations)
        return self

    def _flush(self):
        name = self.collection.name
        self.collection.drop()
        self._create_collections(name)

    def pop(self, key, default=None):
        ret = self._get_item(key)
        if ret is None:
            return default
        _, value = ret
        self._delete_item(key)
        return value

    def popitem(self):
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        key, value = next(self._iter(limit=1))
        self._delete_item(key)
        return value

    def save(self, path):
        target_path = self._get_path(path)
        target_path.unlink(missing_ok=True)
        with target_path.open('wb+') as f:
            for doc in self.collection.find():
                f.write(bson.BSON.encode(doc))
        return path

    def load(self, path):
        path = self._get_path(path)
        data = path.read_bytes()
        examples = bson.decode_all(data)
        self.collection.insert_many(examples)

        return self

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)
