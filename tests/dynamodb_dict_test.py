from spoonbill.datastores import DynamoDBStore
import pytest


@pytest.mark.skip("Run this test manually")
def test_dynamodb():
    store = DynamoDBStore.open('tmp')
    store._flush(delete_table=False)

    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'

    # test set and get
    store.set('another', 'another')
    assert store.get('another') == 'another'
    assert store.get('nope', 'nope') == 'nope'  # test default value
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert list(store.keys(patterns='test')) == ['test']
    assert list(store.items(patterns='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store.set_batch([str(i) for i in range(10)], range(10))
    assert len(store) == 12
    assert set(store.get_batch([i for i in range(10)])) == set(range(10))
    assert len([1 for _ in store]) == 12  # test iterator

# def test_dynamodb_scan():
#     self = store = DynamoDBStore.open('tmp',stict=False)
#     store._flush(delete_table=False)
#     store.update({str(i): {'a': i, 'b':str(i)} for i in range(20)})
#     store.scan('a', 10, 10)
#     self.client.scan(TableName=self.table_name, ExclusiveStartKey=)
#     stmt = f"SELECT * FROM {self.table_name} WHERE a BETWEEN 10 AND 20"
#     [from_dynamodb_json(item) for item in self.client.execute_statement(Statement=stmt)['Items']]
#     from cerealbox.dynamo import from_dynamodb_json
#     import boto3
#
#     self.table.query(KeyConditionExpression=Key('a').between(10, 20))