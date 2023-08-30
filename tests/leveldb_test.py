from tempfile import TemporaryDirectory

from spoonbill.datastores import LevelDBStore


def test_leveldb_strict():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name
    store = LevelDBStore.open(path)

    assert len(store) == 0
    store[b'test'] = b'test'
    assert len(store) == 1
    assert store[b'test'] == store.get(b'test') == b'test'
    # test set and get
    store.set(b'another', b'another')
    assert store.get(b'another') == b'another'

    assert store.get(b'nope', b'nope') == b'nope'  # test default value
    assert b'test' in store  # test contains

    assert set(store.keys()) == set([b'test', b'another'])
    assert set(store.values()) == set([b'test', b'another'])
    assert set(store.items()) == set([(b'test', b'test'), (b'another', b'another')])

    assert list(store.keys(pattern=b'test')) == [b'test']
    assert list(store.items(conditions=b'test')) == [(b'test', b'test')]

    assert store.pop(b'another') == b'another'
    assert len(store) == 1

    assert store.update({b'test': b'test2', b'another': b'another2'})
    assert len(store) == 2
    assert store == {b'test': b'test2', b'another': b'another2'}
    store._flush()
    N = 1000
    store.update({f"{i}".encode(): f"{i}".encode() for i in range(N)})
    assert len(store) == N
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == N  # test iterator


def test_leveldb_save_ingest():
    tmpdir = TemporaryDirectory()
    local_path = tmpdir.name
    store = LevelDBStore.open(local_path)
    store['test'] = 'test'

    ssts = TemporaryDirectory()
    store.save(ssts.name + '/test.sst')
    store._flush()
    assert len(store) == 0
    store.ingest(ssts.name + '/test.sst')
    assert len(store) == 1


def test_leveldb_search():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name
    store = LevelDBStore.open(path)
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='1+')) == {1, '13', '10', '14', '1', '11', '16', '17', '12', '15', '18', '19'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [{'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]


