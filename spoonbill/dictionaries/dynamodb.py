import typing

import contextlib
import logging
from spoonbill.dictionaries import KeyValueStore, KEY, VALUE
import boto3
import decimal
from cerealbox.dynamo import from_dynamodb_json, as_dynamodb_json
import botocore
import time

logger = logging.getLogger()


class DynamoDBDict(KeyValueStore):

    def __init__(self, table_name: str, key_type: str = 'S', **kwargs):
        self.table_name = table_name
        self.client = boto3.client('dynamodb')
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

    def _to_dynamodb_key(self, key):
        return {KEY: as_dynamodb_json(key)}

    def _to_dynamodb_item(self, key, value):

        value = as_dynamodb_json(value)
        return {KEY: {self.key_type: key}, VALUE: value}

    def _from_dynamodb_item(self, item):
        def _convert(item):
            if isinstance(item, decimal.Decimal):
                return float(item)
            if isinstance(item, list):
                return [_convert(i) for i in item]
            if isinstance(item, dict):
                return {k: _convert(v) for k, v in item.items()}
            return item

        return _convert(from_dynamodb_json(item))

    def _get_item(self, key: str):
        response = self.client.get_item(TableName=self.table_name, Key=self._to_dynamodb_key(key))
        return self._from_dynamodb_item(response['Item'][VALUE])

    def _put_item(self, key: str, value: str):
        item = self._to_dynamodb_item(key, value)
        self.client.put_item(TableName=self.table_name, Item=item)

    def _delete_item(self, key: str):
        self.client.delete_item(TableName=self.table_name, Key=self._to_dynamodb_key(key))

    @property
    def description(self):
        return self.client.describe_table(TableName=self.table_name)

    def __len__(self):
        return int(self.client.scan(TableName=self.table_name, Select='COUNT')['Count'])

    def __getitem__(self, item):
        return self._get_item(item)

    def __setitem__(self, key, value):
        return self._put_item(key, value)

    def get(self, key, default=None):
        with contextlib.suppress(KeyError):
            return self._get_item(key)
        return default

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
                    yield self._from_dynamodb_item(item)[KEY]
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
                response = self.client.scan(**params)
                for item in response.get('Items', []):
                    yield self._from_dynamodb_item(item)
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except botocore.exceptions.ClientError as err:
            logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def keys(self, pattern: str = None, count: int = None):
        params = {'TableName': self.table_name, 'Select': 'SPECIFIC_ATTRIBUTES', 'AttributesToGet': [KEY]}
        for item in self._to_iter(self._simple_scan(params, limit=count), pattern=pattern, count=count):
            yield item[KEY]

    def values(self, limit: int = None):
        params = {'TableName': self.table_name, 'Select': 'ALL_ATTRIBUTES'}
        for item in self._simple_scan(params, limit):
            yield item[VALUE]

    def items(self, pattern: str = None, count: int = None):
        params = {'TableName': self.table_name, 'Select': 'ALL_ATTRIBUTES'}
        for item in self._to_iter(self._simple_scan(params, limit=count), pattern=pattern, count=count):
            yield item[KEY], item[VALUE]

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
        with contextlib.suppress(KeyError):
            value = self._get_item(key)
            self._delete_item(key)
            return value
        return default

    def popitem(self):
        for key, value in self.items(limit=1):
            self._delete_item(key)
            return key, value
        raise KeyError('popitem(): dictionary is empty')

    def get_batch(self, keys, default=None):
        responses = self.client.batch_get_item(
            RequestItems={self.table_name: {'Keys': [self._to_dynamodb_key(key) for key in keys]}})
        for item in responses['Responses'][self.table_name]:
            yield self._from_dynamodb_item(item)[VALUE]

    def _insert_items(self, items):
        table = boto3.resource('dynamodb').Table(self.table_name)
        count = 0
        with table.batch_writer() as batch:
            for key, value in items:
                response = batch.put_item(Item={KEY: key, VALUE: value})
                count += 1
        return count

    def update(self, d):
        """
       Insert a batch of items into the table in chunks of 25 items.
       :param d: A dictionary like object with items() method
       :return: Count of items inserted
       """
        return self._insert_items(d.items())

    def set_batch(self, keys, values):
        """
         Insert a batch of items into the table in chunks of 25 items.
         :param d: A dictionary like object with items() method
         :return: Count of items inserted
         """
        return self._insert_items(zip(keys, values))
