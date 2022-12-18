from spoonbill.datastores import ShelveStore
from tempfile import TemporaryDirectory


def test_shelve():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = ShelveStore.open(path)
    store._flush()
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
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}
    store._flush()
    N = 1000
    store.set_batch(range(N), range(N))
    assert len(store) == N
    assert set(store.get_batch(range(10))) == set(range(10))
    assert len([1 for _ in store]) == N  # test iterator

    store['function'] = lambda x: x + 1
    assert store['function'](1) == 2


def test_shelve_save_load():
    tmpdir = TemporaryDirectory()
    local_path = tmpdir.name + '/local.db'
    store = ShelveStore.open(local_path)
    store['test'] = 'test'
    other_path = tmpdir.name + '/cloud.db'
    store.save(other_path)
    store._flush()
    assert len(store) == 0
    store = ShelveStore(local_path).load(other_path)
    assert len(store) == 1
