import typing

from spoonbill.stores import KeyValueStore
import boto3


class DynamoDBHelper(KeyValueStore):

    def __init__(self, key_type='S', value_type='S', strict: bool = False):
        self.key_type = key_type
        self.value_type = value_type
        self.strict = strict

    def to_key(self, key):
        return {'key': {self.key_type: key}}

    def to_item(self, key, value):

        return {'key': {self.key_type: key}, 'value': {self._get_type(value): value}}

    def _get_type(self, value):
        if isinstance(value, str):
            return 'S'
        elif isinstance(value, (int, float)):
            return 'N'
        return 'B'


class DynamoDBDict(KeyValueStore):

    def __init__(self, table_name: str, key_type: str = 'S', value_type: str = 'S' ** kwargs):
        self.helper = DynamoDBHelper(key_type=key_type, value_type=value_type)
        self.table_name = table_name
        self.client = boto3.client('dynamodb')

    def open(self, table_name: str, **kwargs):
        pass

    def from_table(self, table_name):
        client = boto3.client('dynamodb')
        description = client.describe_table(TableName=table_name)
        key_type = description['Table']['AttributeDefinitions'][0]['AttributeType']
        # if tabledoesnt exists error
        # find key and value types for helper

        return DynamoDBDict(table_name=table_name)

    def _list_tables(self):
        return self.client.list_tables()['TableNames']

    def _create_table(self, table_name: str = None,
                      key_schema: typing.List[dict] = [
                          {'AttributeName': 'key', 'KeyType': 'HASH'}],  # Partition key
                      attribute_definitions: typing.List[dict] = [{'AttributeName': 'key', 'AttributeType': 'S'}],

                      **kwargs):
        table_name = table_name or self.table_name
        if not table_name in self._list_tables():
            billing = kwargs.get('billing_mode', 'PAY_PER_REQUEST')
            return self.client.create_table(TableName=table_name, KeySchema=key_schema,
                                            AttributeDefinitions=attribute_definitions,
                                            BillingMode=billing,

                                            **kwargs)
        return False

    def _delete_table(self, table_name: str = None):
        name = table_name or self.table_name
        if name not in self._list_tables():
            return False
        return self.client.delete_table(TableName=name)

    def _table_exists(self, name: str):
        return name in client.list_tables()['TableNames']

    def _get_item(self, key: str):
        return self.client.get_item(TableName=self.name, Key=self.helper.to_key(key))['Item']['value']['S']

    def _put_item(self, key: str, value: str):
        item = self.helper.to_item(key, value)
        self.client.put_item(TableName=self.name, Item=item)

    def _delete_item(self, key: str):
        self.client.delete_item(TableName=self.name, Key=self.helper.to_key(key))

    @property
    def description(self):
        return self.client.describe_table(TableName=self.name)

    def __len__(self):
        return int(self.client.scan(TableName=self.name, Select='COUNT')['Count'])


client = boto3.client('dynamodb')
table_name = 'tmp'
key_schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
attribute_definitions = [{'AttributeName': 'id', 'AttributeType': 'S'}]

self = DynamoDBDict(table_name)
self._get_item('test')

# self._delete_table()
# self._create_table()
# a._list_tables()
# self._put_item('test', 'test')
# self._put_item('test2', 'test2')
# a._get_item('test')
# key = 'test'
# value = 'test'
