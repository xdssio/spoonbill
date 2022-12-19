from spoonbill.datastores import CosmosDBStore
import time
import pytest


@pytest.mark.skip("Run this test manually")
def test_cosmos_strict():
    store = CosmosDBStore.open('tmp', strict=True)
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

    assert list(store.keys(patterns='test')) == ['test']
    assert list(store.items(patterns='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store.set_batch([str(i) for i in range(10)], range(10))
    assert len(store) == 12
    assert set(store.get_batch([str(i) for i in range(10)])) == set(range(10))
    assert len([1 for _ in store]) == 12  # test iterator

    store['stuff'] = {'a': 1, 'b': 2}
    store._flush()


def test_cosmos():
    store = CosmosDBStore.open('tmp', strict=False)
    store['function'] = lambda x: x
    assert store['function'](1) == 1
    store._flush()
    # store.client.delete_database(store.database_name)


def test_cosmos_search():
    store = CosmosDBStore.open(strict=True)
    store._flush()
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})

    assert list(store.items(patterns={'b': '1', 'a': 1})) == [('1', {'a': 1, 'b': '1'})]
    assert list(store.keys(pattern='1%')) == ['1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '21']
    assert list(store.values(patterns={'b': '2%'})) == [{'a': 2, 'b': '2'}, {'a': 12, 'b': '12'}, {'a': 20, 'b': '20'},
                                                        {'a': 21, 'b': '21'}]
