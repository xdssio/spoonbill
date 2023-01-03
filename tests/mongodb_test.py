from tempfile import TemporaryDirectory

from spoonbill.datastores import MongoDBStore


def test_mongodb_strict():
    self = store = MongoDBStore.open()
    store._flush()
    len(store)

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
    assert list(store.items(conditions='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store.update({str(i): i for i in range(10)})
    assert len(store) == 12
    assert set(store.values([str(i) for i in range(10)])) == set(range(10))
    assert len([1 for _ in store]) == 12  # test iterator
    store['data'] = {'feature1': 1, 'feature2': 'male'}
    assert store['data'] == {'feature1': 1, 'feature2': 'male'}


def test_mongodb_not_strict():
    store = MongoDBStore.open(strict=False)
    store._flush()
    store['function'] = lambda x: x + 1
    assert store['function'](1) == 2


def test_mongodb_search():
    store = MongoDBStore.open(strict=True)
    store._flush()
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='^1+')) == {'13', '10', '14', '1', '11', '16', '17', '12', '15', '18', '19'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [{'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]


def mongodb_save_load():
    tmpdir = TemporaryDirectory()
    store = MongoDBStore.open(strict=True)
    store._flush()
    N = 1000
    store.update({i: {str(i): i} for i in range(N)})
    path = tmpdir.name + '/cloud.db'
    store.save(path)
    store._flush()
    assert len(store) == 0
    store = MongoDBStore().load(path)
    assert len(store) == N
