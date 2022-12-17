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
    store.set_batch(range(10), range(10))
    assert len(store) == 10
    assert set(store.get_batch(range(10))) == set(range(10))
    assert len([1 for _ in store]) == 10  # test iterator

    store['function'] = lambda x: x + 1
    assert store['function'](1) == 2
