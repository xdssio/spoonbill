from tempfile import TemporaryDirectory
from spoonbill.datastores.jsonstore import JsonStore
import pytest


def test_jsondict():
    tmpdir = TemporaryDirectory()
    store = JsonStore.open(tmpdir.name + '/tmp.json',
                           strict=True,
                           lockfile_path=tmpdir.name+'/tmp.lock',
                           use_jsonpickle=True)
    store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
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
    assert store.popitem()

    store._flush()
    store.strict = False
    N = 11
    store.update({i: i for i in range(N)})
    assert len(store) == N
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == N  # test iterator


@pytest.mark.skip("Run manually")
def test_jsondicks3():
    path = 's3://xdss-tmp/tmp.json'
    lock = 's3://xdss-tmp/tmp.lock'
    store = JsonStore.open(path, lockfile_path=lock)
    store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains
