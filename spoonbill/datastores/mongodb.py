import re
from spoonbill.datastores import KeyValueStore, VALUE, KEY
from pymongo import MongoClient
import pymongo
import bson
import cloudpickle

ID = '_id'


class MongoDBStore(KeyValueStore):

    def __init__(self, uri: str = None, database: str = 'db', collection: str = 'collection', strict=True):

        self.client = MongoClient(uri) if uri else MongoClient()
        self.database = self.client[database]
        self.collection = None
        self.strict = strict
        self.as_string = False
        self._create_collections(collection)

    def _create_collections(self, collection):
        self.collection = self.database[collection]
        self.collection.create_index('key', unique=True)

    @classmethod
    def open(cls, uri: str = None, database: str = 'db', collection: str = 'collection', strict=True):
        return MongoDBStore(uri=uri, database=database, collection=collection, strict=strict)

    def _list_collections(self):
        return self.database.list_collection_names()

    def _to_item_value(self, value):
        if isinstance(value, dict):
            return value
        return {VALUE: self.decode_value(value)}

    def _to_item(self, key, value):
        key, value = self.encode_key(key), self._to_item_value(value)
        item = {KEY: key, ID: key}
        item.update(value)
        return item

    def _from_item(self, item):
        if item is None:
            return None
        key = self.decode_key(item.pop(KEY))
        _ = item.pop(ID, None)
        value = item.get(VALUE) if isinstance(item, dict) and VALUE in item else item
        value = self.decode_value(value)
        return key, value

    def _put_item(self, key, value):
        self.collection.insert_one(self._to_item(key, value))

    def _get_item(self, key):
        return self._from_item(self.collection.find_one({ID: self.encode_key(key)}))

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

    def _iter(self, pattern: str = None, limit: int = None):
        params = {}
        if pattern is not None:
            pattern = re.compile(pattern, re.IGNORECASE)
            params = {KEY: pattern}
        return self.collection.find(**params).limit(limit) if limit else self.collection.find(params)

    def keys(self, pattern: str = None, limit: int = None):
        for item in self._iter(pattern=pattern, limit=limit):
            key, _ = self._from_item(item)
            yield key

    def items(self, pattern: str = None, limit: int = None):
        for item in self._iter(pattern=pattern, limit=limit):
            key, value = self._from_item(item)
            yield key, value

    def values(self, pattern: str = None, limit: int = None):
        for item in self._iter(pattern=pattern, limit=limit):
            _, value = self._from_item(item)
            yield value

    def _update(self, items):
        operations = []
        for key, value in items:
            item = self._to_item(key, value)
            operations.append(
                pymongo.ReplaceOne(filter={ID: item.pop(ID)},
                                   replacement=item, upsert=True))
        self.collection.bulk_write(operations)
        return self

    def update(self, d):
        self._update(d.items())
        return self

    def get_batch(self, keys, default=None):
        encoded_keys = [self.encode_key(key) for key in keys]
        for item in self.collection.find({ID: {'$in': encoded_keys}}):
            key, value = self._from_item(item)
            yield value

    def set_batch(self, keys, values):
        self._update(zip(keys, values))

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
        # operations = []
        # for doc in examples:
        #     key = doc.pop(KEY)
        #     operations.append(
        #         pymongo.ReplaceOne(filter={ID: key},
        #                            replacement={VALUE: value, KEY: key}, upsert=True))
        # self.collection.bulk_write(operations)

        return self
