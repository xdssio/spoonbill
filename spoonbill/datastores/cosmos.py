import os
import contextlib
from spoonbill.datastores import KeyValueStore, VALUE
from azure.cosmos import CosmosClient, PartitionKey
import azure.cosmos.exceptions
import json

ID = 'id'
KEY = '_KEY_'

DEFAULT_PARTITION_KEY = '/id'


class CosmosDBStore(KeyValueStore):
    """
    Azure Cosmos DB Key-value store.
    """

    def __init__(self, database: str = 'db',
                 container: str = 'container',
                 endpoint: str = None,
                 credential: str = None,
                 partition_key=DEFAULT_PARTITION_KEY,
                 strict=True):
        self.client = CosmosClient(url=endpoint or os.getenv('COSMOS_ENDPOINT'),
                                   credential=credential or os.getenv('COSMOS_KEY'))
        self.database_name = database
        self.database = self.client.create_database_if_not_exists(id=self.database_name)
        self.container = self.database.create_container_if_not_exists(
            id=container, partition_key=PartitionKey(path=partition_key), offer_throughput=400)
        self.strict = strict
        self.as_string = True

    @classmethod
    def open(cls, database: str = 'db', container: str = 'container', endpoint: str = None, credential: str = None,
             partition_key=DEFAULT_PARTITION_KEY, strict=True):
        return CosmosDBStore(database=database, container=container, endpoint=endpoint, credential=credential,
                             partition_key=partition_key,
                             strict=strict)

    def _list_containers(self):
        return [container['id'] for container in self.database.list_containers()]

    def _to_item(self, key, value):
        item = {ID: self.encode_key(key)}
        if not self.strict or not isinstance(value, dict):
            item[VALUE] = self.encode_value(value)
        else:
            item.update(value)
        return item

    def _to_key_value(self, item):
        if item is not None:
            key = self.decode_value(item.pop(ID))
            if VALUE in item:
                value = self.decode_value(item[VALUE])
            else:
                value = {k: v for k, v in item.items() if not str(k).startswith('_')}
            key = self.decode_key(key)
            return key, value

    def _put_item(self, key, value):
        self.container.create_item(self._to_item(key, value))

    def _get_item(self, key):
        key = self.encode_key(key)
        with contextlib.suppress(azure.cosmos.exceptions.CosmosResourceNotFoundError):
            return self._to_key_value(self.container.read_item(item=key, partition_key=key))

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

    def _iter_items(self, conditions: dict = None, limit: int = None):
        conditions = conditions or {}
        wheres = []
        for feature, pattern in conditions.items():
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
            yield self._to_key_value(item)

    def _iter_keys(self, where: str = None, limit: int = None):
        query = f"SELECT * FROM {self.container.id} c"
        if where:
            query = query + where
        if limit:
            query = query + f" OFFSET 0 LIMIT {limit}"
        for item in self.container.query_items(
                query=query,
                enable_cross_partition_query=True
        ):
            yield self._to_key_value(item)

    def keys(self, pattern: str = None, limit: int = None):
        where = ' WHERE ' + 'c.id LIKE "%{}%"'.format(pattern) if pattern else None
        for item in self._iter_keys(where=where, limit=limit):
            yield item[0]

    def items(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}
        for item in self._iter_items(conditions=conditions, limit=limit):
            yield item

    def _scan_by_keys(self, keys: list, default=None):
        order = {i: key for i, key in enumerate(keys)}
        group = json.dumps(keys)
        group = '(' + group[1:-1] + ')'
        where = ' WHERE ' + 'c.id IN {}'.format(group) if group else None
        items = {item[0]: item[1] for item in self._iter_keys(where=where)}
        for i in range(len(order)):
            yield items.get(order.get(i), default)

    def values(self, keys: list = None, limit: int = None, default=None):
        if keys:
            for value in self._scan_by_keys(keys=keys, default=default):
                yield value
        else:
            for item in self._iter_items(limit=limit):
                yield item[1]

    def update(self, d):
        for key, value in d.items():
            self[key] = value
        return self

    def _flush(self):
        for key, value in self._iter_items():
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
        item = next(self._iter_items(limit=1))
        self._delete_item(item.get(ID))
        _, value = self._to_key_value(item)
        return value

    def encode_key(self, key):
        if self.strict:
            return key
        return str(key)

    def decode_key(self, key):
        if self.strict:
            return key
        return key
