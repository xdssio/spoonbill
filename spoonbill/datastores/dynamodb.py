import typing
import logging
from spoonbill.datastores import KeyValueStore, KEY, VALUE
import boto3
import decimal
import botocore
import time

logger = logging.getLogger()

ITEM = 'Item'
DYNAMODB = 'dynamodb'



class DynamoDBDict(KeyValueStore):

    def __init__(self, table_name: str, key_type: str = 'S', **kwargs):
        self.table_name = table_name
        self.client = boto3.client(DYNAMODB, **kwargs)
        self.table = boto3.resource(DYNAMODB, **kwargs).Table(self.table_name)
        self.strict = True
        self.key_type = key_type
        self.create_table()

    @classmethod
    def open(self, table_name: str, key_type: str = None, **kwargs):
        client = boto3.client('dynamodb', **kwargs)
        if key_type is None:
            try:
                description = client.describe_table(TableName='asf')
                key_type = description['Table']['AttributeDefinitions'][0]['AttributeType']
            except:
                key_type = 'S'
        return DynamoDBDict(table_name=table_name, key_type=key_type, **kwargs)

    def _list_tables(self):
        return self.client.list_tables()['TableNames']

    def create_table(self, table_name: str = None,
                     key_schema: typing.List[dict] = None,  # Partition key
                     attribute_definitions: typing.List[dict] = None,
                     billing_mode: str = None,
                     **kwargs):
        table_name = table_name or self.table_name

        if not table_name in self._list_tables():
            key_schema = key_schema or kwargs.pop('AttributeDefinitions', [{'AttributeName': KEY, 'KeyType': 'HASH'}])
            attribute_definitions = attribute_definitions or kwargs.pop('AttributeDefinitions', [
                {'AttributeName': KEY, 'AttributeType': self.key_type}])
            billing_mode = billing_mode or kwargs.pop('BillingMode', 'PAY_PER_REQUEST')
            return self.client.create_table(TableName=table_name, KeySchema=key_schema,
                                            AttributeDefinitions=attribute_definitions,
                                            BillingMode=billing_mode,
                                            **kwargs)
        return False

    def _delete_table(self, table_name: str = None):
        name = table_name or self.table_name
        if name not in self._list_tables():
            return False
        return self.client.delete_table(TableName=name)

    def _table_exists(self, name: str):
        return name in self.client.list_tables()['TableNames']

    @staticmethod
    def _process_item(item):
        if VALUE in item:
            item = item[VALUE]

        def _convert(item):
            if isinstance(item, decimal.Decimal):
                return float(item)
            if isinstance(item, list):
                return [_convert(i) for i in item]
            if isinstance(item, dict):
                return {k: _convert(v) for k, v in item.items()}
            return item

        return _convert(item)

    def _to_key_type(self, key):
        if self.key_type == 'S':
            key = str(key)
        elif self.key_type == 'N':
            key = float(key)
        return key

    def _to_key(self, key):
        return {KEY: self._to_key_type(key)}

    def _to_item(self, key, value):
        if isinstance(value, dict):
            return {**{KEY: key}, **value}
        return {KEY: key, VALUE: value}

    def _from_item(self, item):
        key = item.pop(KEY)

        return key, self._process_item(item)

    def _get_item(self, key: str):
        response = self.table.get_item(TableName=self.table_name, Key=self._to_key(key))
        if ITEM in response:
            return self._from_item(response[ITEM])

    def _put_item(self, key: str, value: str):
        self.table.put_item(TableName=self.table_name, Item=self._to_item(key, value))

    def _delete_item(self, key: str):
        self.table.delete_item(TableName=self.table_name, Key=self._to_key(key))

    @property
    def description(self):
        return self.client.describe_table(TableName=self.table_name)

    def __len__(self):
        return int(self.client.scan(TableName=self.table_name, Select='COUNT')['Count'])

    def __contains__(self, item):
        return self._get_item(item) is not None

    def __getitem__(self, item):
        item = self._get_item(item)
        if item is None:
            raise KeyError(item)
        return item[1]

    def __setitem__(self, key, value):
        return self._put_item(key, value)

    def __delitem__(self, key):
        return self._delete_item(key)

    def get(self, key, default=None):
        item = self._get_item(key)
        if item is None:
            return default
        return item[1]

    def set(self, key, value):
        return self._put_item(key, value)

    def scan(self, **kwargs):
        params = {**{'TableName': self.table_name}, **kwargs}
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    params['ExclusiveStartKey'] = start_key
                response = self.client.scan(**params)
                for item in response.get('Items', []):
                    yield self._from_item(item)
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def _simple_scan(self, params: dict, limit: int = None):
        if limit:
            params['Limit'] = limit
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    params['ExclusiveStartKey'] = start_key
                response = self.table.scan(**params)
                for item in response.get('Items', []):
                    yield self._from_item(item)
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def keys(self, pattern: str = None, limit: int = None):
        params = {'TableName': self.table_name, 'Select': 'SPECIFIC_ATTRIBUTES', 'AttributesToGet': [KEY]}
        for item in self._to_iter(self._simple_scan(params, limit=limit), pattern=pattern, limit=limit):
            yield item[0]

    def values(self, limit: int = None):
        params = {'TableName': self.table_name, 'Select': 'ALL_ATTRIBUTES'}
        for item in self._simple_scan(params, limit):
            yield item[1]

    def items(self, pattern: str = None, limit: int = None):
        params = {'TableName': self.table_name, 'Select': 'ALL_ATTRIBUTES'}
        for item in self._to_iter(self._simple_scan(params, limit=limit), pattern=pattern, limit=limit):
            yield item

    def _flush(self):
        description = self.description['Table']
        count = len(self)
        self._delete_table(self.table_name)
        time.sleep(2)
        self.create_table(table_name=self.table_name,
                          key_schema=description['KeySchema'],
                          attribute_definitions=description['AttributeDefinitions'],
                          billing_mode=description['BillingModeSummary']['BillingMode'])
        return count

    def pop(self, key, default=None):
        _, value = self._get_item(key)
        if value:
            self._delete_item(key)
            return value
        return default

    def popitem(self):
        for key, value in self.items(limit=1):
            self._delete_item(key)
            return key, value
        raise KeyError('popitem(): dictionary is empty')

    def _to_dynamodb_key(self, key):
        return {KEY: {self.key_type: self._to_key_type(key)}}


    def get_batch(self, keys, default=None):
        from cerealbox.dynamo import from_dynamodb_json
        responses = self.client.batch_get_item(
            RequestItems={self.table_name: {'Keys': [self._to_dynamodb_key(key) for key in keys]}})
        for item in responses['Responses'][self.table_name]:
            yield self._from_item(from_dynamodb_json(item))[1]

    def _insert_items(self, items):
        count = 0
        with self.table.batch_writer() as batch:
            for key, value in items:
                response = batch.put_item(Item=self._to_item(key, value))
                count += 1
        return count

    def update(self, d):
        """
       Insert a batch of items into the table in chunks of 25 items.
       :param d: A dictionary like object with items() method
       :return: Count of items inserted
       """
        self._insert_items(d.items())
        return self

    def set_batch(self, keys, values):
        """
         Insert a batch of items into the table in chunks of 25 items.
         :param d: A dictionary like object with items() method
         :return: Count of items inserted
         """
        return self._insert_items(zip(keys, values))
