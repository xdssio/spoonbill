import os
import contextlib
from spoonbill.dictionaries import KeyValueStore, VALUE, KEY
from azure.cosmos import CosmosClient, PartitionKey
import azure.cosmos.exceptions


class CosmosDBDict(KeyValueStore):

    def __init__(self, database: str = 'db', container: str = 'container', endpoint: str = None, credential: str = None):
        self.client = CosmosClient(url=endpoint or os.getenv('COSMOS_ENDPOINT'),
                                   credential=credential or os.getenv('COSMOS_KEY'))
        self.database_name = database
        self.partition_key_path = PartitionKey(path="/key")
        self.database = self.client.create_database_if_not_exists(id=self.database_name)
        self.container = self.database.create_container_if_not_exists(
            id=container, partition_key=self.partition_key_path, offer_throughput=400)
        self.strict = False

    @classmethod
    def open(cls, database: str = 'db', container: str = 'container', endpoint: str = None, credential: str = None):
        return CosmosDBDict(database=database, container=container, endpoint=endpoint, credential=credential)

    def _list_containers(self):
        return [container['id'] for container in self.database.list_containers()]

    def _put_item(self, key, value):
        key, value = self.encode_key(key), self.encode_value(value)
        self.container.create_item({KEY: key, 'id': key, VALUE: value})

    def _get_item(self, key):
        key = self.encode_key(key)
        with contextlib.suppress(azure.cosmos.exceptions.CosmosResourceNotFoundError):
            return self.container.read_item(item=key, partition_key=key)
        return None

    def __len__(self):
        return next(
            self.container.query_items(query='SELECT VALUE COUNT(c.id) from c', enable_cross_partition_query=True))

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
            item[VALUE] = self.encode_value(value)
            self.container.upsert_item(item)

    def _delete_item(self, key):
        with contextlib.suppress(azure.cosmos.exceptions.CosmosResourceNotFoundError):
            self.container.delete_item(item=key, partition_key=key)

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
        query = f"SELECT * FROM {self.container.id} c"
        if pattern:
            query = query + ' WHERE c.key LIKE "%{}%"'.format(pattern)
        if count:
            query = query + f" OFFSET 0 LIMIT {count}"
        for item in self.container.query_items(
                query=query,
                enable_cross_partition_query=True
        ):
            yield item

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
        for key, value in items:
            self[key] = value

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
        for item in self._iter():
            self.container.delete_item(item.get('id'), partition_key=item.get('id'))

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
