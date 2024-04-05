from tempfile import TemporaryDirectory
from spoonbill.datastores.rocksdb import RocksDBStore


def test_rocksdb_strict():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name
    store = RocksDBStore.open(path)
    assert len(store) == 0
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
    assert set(store.items()) == set(
        [('test', 'test'), ('another', 'another')])

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.items(conditions='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}
    store._flush()
    N = 1000
    store.update({i: i for i in range(N)})
    assert len(store) == N
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == N  # test iterator


def test_rocksdb_save_ingest():
    tmpdir = TemporaryDirectory()
    local_path = tmpdir.name
    store = RocksDBStore.open(local_path)
    store['test'] = 'test'

    ssts = TemporaryDirectory()
    store.save(ssts.name + '/test.sst')
    store._flush()
    assert len(store) == 0
    store.ingest(ssts.name + '/test.sst')
    assert len(store) == 1


def test_rocksdb_search():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name
    store = RocksDBStore.open(path)
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [
        ('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='1+')
               ) == {1, '13', '10', '14', '1', '11', '16', '17', '12', '15', '18', '19'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [
        {'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]
