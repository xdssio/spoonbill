from spoonbill.dictionaries import CosmosDBDict
import time
import pytest


@pytest.mark.skip("Run this test manually")
def test_cosmos():
    self = store = CosmosDBDict.open('tmp')
    store._flush()
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

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.items(pattern='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store.set_batch([str(i) for i in range(10)], range(10))
    assert len(store) == 12
    assert set(store.get_batch([str(i) for i in range(10)])) == set(range(10))
    assert len([1 for _ in store]) == 12  # test iterator

    store['function'] = lambda x: x
    assert store['function'](1) == 1
    store._flush()
    # store.client.delete_database(store.database_name)
