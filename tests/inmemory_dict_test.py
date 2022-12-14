from tempfile import TemporaryDirectory

from spoonbill import InMemoryDict


def test_dict():
    store = InMemoryDict.open()
    assert store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')

    for key in store.keys():  # test contains
        assert key in store

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}
    store._flush()
    store = InMemoryDict()
    store.set_batch(range(10), range(10))
    assert len(store) == 10
    assert list(store.get_batch(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 10  # test iterator

    assert list(store.scan('1*')) == list(range(10))  # scan looks at keys as strings


def test_inmemory_save_load():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = InMemoryDict.open()
    store['test'] = 'test'
    store.save(path)
    store._flush()
    assert len(store) == 0
    store = InMemoryDict.load(path)
    assert len(store) == 1
