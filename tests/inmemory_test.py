from tempfile import TemporaryDirectory

from spoonbill.datastores import InMemoryStore, FilesystemStore


def test_dict_strict():
    store = InMemoryStore.open()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
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


def test_inmemory_dict():
    store = InMemoryStore(strict=False)
    store.update({i: i for i in range(11)})
    assert len(store) == 11
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 11  # test iterator

    store = InMemoryStore(strict=True)
    store.update({i: i for i in range(11)})
    assert [item for item in store.keys(pattern='1+')] == [1, 10]  # scan looks at keys as strings


def test_inmemory_search():
    store = InMemoryStore.open(strict=True)
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='1+')) == {1, '13', '10', '14', '1', '11', '16', '17', '12', '15', '18', '19'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [{'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]


def test_inmemory_save_load():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = InMemoryStore.open()
    store['test'] = 'test'
    store.save(path)
    store._flush()
    assert len(store) == 0
    store = InMemoryStore().load(path)
    assert len(store) == 1

    store = InMemoryStore({'1': '1', '2': '2'})
    len(InMemoryStore.open(store)) == 2

    bucket = FilesystemStore(tmpdir.name + '/bucket.db')
    bucket.update({'1': '1', '2': '2'})
    assert len(InMemoryStore.open(bucket)) == 2

    bucket.save(tmpdir.name + '/dir')
    assert len(InMemoryStore.open(tmpdir.name + '/dir')) == 2


def test_inmemory_ordereddict():
    from collections import OrderedDict
    store = InMemoryStore.open(OrderedDict())
    store.update({key: key for key in range(1000)})
    assert list(store.keys()) == list(range(1000))
