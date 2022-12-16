import re
from spoonbill.dictionaries import KeyValueStore, VALUE, KEY
from pymongo import MongoClient
import pymongo
ID = '_id'


class MongoDBDict(KeyValueStore):

    def __init__(self, database: str = 'db', collection: str = 'collection'):
        self.client = MongoClient()
        self.database = self.client[database]
        self.collection = None
        self.strict = False
        self._create_collections(collection)

    def _create_collections(self, collection):
        self.collection = self.database[collection]
        self.collection.create_index('key', unique=True)

    @classmethod
    def open(cls, database: str = 'db', collection: str = 'container'):
        return MongoDBDict(database=database, collection=collection)

    def _list_collections(self):
        return self.database.list_collection_names()

    def _put_item(self, key, value):
        key, value = self.encode_key(key), self.encode_value(value)
        self.collection.insert_one({KEY: key, ID: key, VALUE: value})

    def _get_item(self, key):
        return self.collection.find_one({ID: self.encode_key(key)})

    def __len__(self):
        return self.collection.count_documents({})

    def __contains__(self, item):
        return self._get_item(item) is not None

    def __getitem__(self, item):
        value = self._get_item(item)
        if value is None:
            raise KeyError(item)
        return self.decode_value(value.get(VALUE))

    def __setitem__(self, key, value):
        item = self._get_item(key)
        if item is None:
            self._put_item(key, value)
        else:
            self.collection.update_one({ID: item.get(ID)}, {"$set": {VALUE: self.encode_value(value)}})

    def _delete_item(self, key):
        self.collection.delete_one({ID: self.encode_key(key)})

    def __delitem__(self, key):
        self._delete_item(key)

    def get(self, item, default=None):
        item = self._get_item(item)
        if item is None:
            return default
        return self.decode_value(item.get(VALUE))

    def set(self, key, value):
        self._put_item(key, value)

    def _iter(self, pattern: str = None, count: int = None):
        params = {}
        if pattern is not None:
            pattern = re.compile(pattern, re.IGNORECASE)
            params = {KEY: pattern}
        return self.collection.find(**params).limit(count) if count else self.collection.find(params)

    def keys(self, pattern: str = None, count: int = None):
        for item in self._iter(pattern=pattern, count=count):
            yield self.decode_key(item.get(KEY))

    def items(self, pattern: str = None, count: int = None):
        for item in self._iter(pattern=pattern, count=count):
            yield self.decode_key(item.get(KEY)), self.decode_value(item.get(VALUE))

    def values(self, pattern: str = None, count: int = None):
        for item in self._iter(pattern=pattern, count=count):
            yield self.decode_key(item.get(VALUE))

    def _update(self, items):
        bulk = self.collection.bulk_write()

        for id in ids:
            bulk.find({'_id': id}).update({'$set': {"isBad": "N"}})

        bulk.execute()
        self.collection.insert_many(
            [{KEY: self.encode_key(key),
              ID: self.encode_key(key),
              VALUE: self.encode_value(value)} for key, value in
             items])

    def update(self, d):
        self._update(d.items())
        return self

    def get_batch(self, keys, default=None):
        for key in keys:
            yield self.get(key, default)

    def set_batch(self, keys, values):
        for key, value in zip(keys, values):
            self[key] = value

    def _flush(self):
        self.collection.bulk_write([pymongo.DeleteMany({})])
        name = self.collection.name
        self.collection.drop()
        self._create_collections(name)

    def pop(self, key, default=None):
        ret = self._get_item(key)
        if ret is None:
            return default
        self._delete_item(key)
        return self.decode_value(ret.get(VALUE))

    def popitem(self):
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        item = next(self._iter(count=1))
        value = self.decode_value(item.get(VALUE))
        self._delete_item(item.get(KEY))
        return value
