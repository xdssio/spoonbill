from tempfile import TemporaryDirectory

from spoonbill.datastores.leveldb import LevelDBStore
import pytest


@pytest.mark.xfail(reason="Cloud setup required - cuurently disabled. Run manually.")
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
    assert set(store.items()) == set(
        [(b'test', b'test'), (b'another', b'another')])

    assert list(store.keys(pattern=b'test')) == [b'test']
    assert list(store.items(conditions=b'test')) == [(b'test', b'test')]

    assert store.pop(b'another') == b'another'
    assert len(store) == 1

    assert store.update({b'test': b'test2', b'another': b'another2'})
    assert len(store) == 2
    assert store == {b'test': b'test2', b'another': b'another2'}
    store._flush()
    len(store)
    N = 1000
    store.update({f"{i}".encode(): f"{i}".encode() for i in range(N)})
    assert len(store) == N
    assert list(store.values([str(i).encode() for i in range(10)])) == list(
        store.values([str(i).encode() for i in range(10)]))
    assert len([1 for _ in store]) == N  # test iterator

    store['None'] = None
    assert 'None' in store
    assert store.get('None') is None
    assert b'BLABLA' not in store  # valid contains for None value
    assert b'0' in store  # valid contains for None value


@pytest.mark.xfail(reason="Cloud setup required - cuurently disabled. Run manually.")
def test_leveldb_not_strict():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name
    store = LevelDBStore.open(path, strict=False)

    assert len(store) == 0
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    # test set and get
    store.set('another', 'another')
    assert store.get('another') == 'another'
    assert len(store) == 2
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
    len(store)
    N = 1000
    store.update({i: i for i in range(N)})
    assert len(store) == N
    assert list(store.values([i for i in range(10)])) == list(
        store.values([i for i in range(10)]))
    assert len([1 for _ in store]) == N  # test iterator

    store['None'] = None
    assert 'None' in store
    assert store.get('None') is None
    assert 'BLABLA' not in store  # valid contains for None value


@pytest.mark.xfail(reason="Cloud setup required - cuurently disabled. Run manually.")
def test_leveldb_search():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name
    store = LevelDBStore.open(path, strict=False)
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [
        ('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='1+')
               ) == {1, '13', '10', '14', '1', '11', '16', '17', '12', '15', '18', '19'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [
        {'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]
