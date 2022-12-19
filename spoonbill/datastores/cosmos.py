import os
import contextlib
from spoonbill.datastores import KeyValueStore, VALUE, KEY
from azure.cosmos import CosmosClient, PartitionKey
import azure.cosmos.exceptions


class CosmosDBStore(KeyValueStore):
    ID = 'id'

    def __init__(self, database: str = 'db', container: str = 'container', endpoint: str = None, credential: str = None,
                 strict=True):
        self.client = CosmosClient(url=endpoint or os.getenv('COSMOS_ENDPOINT'),
                                   credential=credential or os.getenv('COSMOS_KEY'))
        self.database_name = database
        self.partition_key_path = PartitionKey(path="/key")
        self.database = self.client.create_database_if_not_exists(id=self.database_name)
        self.container = self.database.create_container_if_not_exists(
            id=container, partition_key=self.partition_key_path, offer_throughput=400)
        self.strict = strict
        self.as_string = True

    @classmethod
    def open(cls, database: str = 'db', container: str = 'container', endpoint: str = None, credential: str = None,
             strict=True):
        return CosmosDBStore(database=database, container=container, endpoint=endpoint, credential=credential,
                             strict=strict)

    def _list_containers(self):
        return [container['id'] for container in self.database.list_containers()]

    def _to_item(self, key, value):
        key = self.encode_key(key)
        item = {KEY: key, CosmosDBStore.ID: key}
        if not self.strict or not isinstance(value, dict):
            item[VALUE] = self.encode_value(value)
        else:
            item.update(value)
        return item

    def _from_item(self, item):
        if item is not None:
            key = item.pop(KEY)
            if VALUE in item:
                value = self.decode_value(item[VALUE])
            else:
                value = {k: v for k, v in item.items() if not str(k).startswith('_')}
                value.pop(CosmosDBStore.ID, None)
            key = self.decode_key(key)
            return key, value

    def _put_item(self, key, value):
        self.container.create_item(self._to_item(key, value))

    def _get_item(self, key):
        key = self.encode_key(key)
        with contextlib.suppress(azure.cosmos.exceptions.CosmosResourceNotFoundError):
            return self._from_item(self.container.read_item(item=key, partition_key=key))

    def __len__(self):
        return next(
            self.container.query_items(query='SELECT VALUE COUNT(c.id) from c', enable_cross_partition_query=True))

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
            self.container.upsert_item(self._to_item(key, value))

    def _delete_item(self, key):
        key = self.encode_key(key)
        with contextlib.suppress(azure.cosmos.exceptions.CosmosResourceNotFoundError):
            self.container.delete_item(item=key, partition_key=key)

    def __delitem__(self, key):
        self._delete_item(key)

    def get(self, item, default=None):
        item = self._get_item(item)
        if item is None:
            return default
        return item[1]

    def set(self, key, value):
        self._put_item(key, value)

    def _iter(self, patterns: dict = None, limit: int = None):
        patterns = patterns or {}
        wheres = []
        for feature, pattern in patterns.items():
            if isinstance(pattern, str):
                wheres.append('c.{} LIKE "%{}%"'.format(feature, pattern))
            else:
                wheres.append(f'c.{feature} = {pattern}'.format(feature, pattern))
        query = f"SELECT * FROM {self.container.id} c"
        if wheres:
            query = query + ' WHERE ' + ' AND '.join(wheres)
        if limit:
            query = query + f" OFFSET 0 LIMIT {limit}"
        for item in self.container.query_items(
                query=query,
                enable_cross_partition_query=True
        ):
            yield self._from_item(item)

    def keys(self, pattern: str = None, limit: int = None):
        patterns = {KEY: pattern} if pattern else None
        for item in self._iter(patterns=patterns, limit=limit):
            yield item[0]

    def items(self, patterns: dict = None, limit: int = None):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {VALUE: patterns}
        for item in self._iter(patterns=patterns, limit=limit):
            yield item

    def values(self, patterns: dict = None, limit: int = None):
        if patterns is not None and not hasattr(patterns, 'items'):
            patterns = {KEY: patterns}
        for item in self._iter(patterns=patterns, limit=limit):
            yield item[1]

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
        for key,value in self._iter():
            self.container.delete_item(key, partition_key=key)

    def pop(self, key, default=None):
        ret = self._get_item(key)
        if ret is None:
            return default
        self._delete_item(key)
        return ret[1]

    def popitem(self):
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        item = next(self._iter(limit=1))
        self._delete_item(item.get(KEY))
        _, value = self._from_item(item)
        return value

    def encode_key(self, key):
        if self.strict:
            return key
        return str(key)

    def decode_key(self, key):
        if self.strict:
            return key
        return key
