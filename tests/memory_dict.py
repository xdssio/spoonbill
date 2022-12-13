from spoonbill import MemoryKeyStore, MemoryStringStore


def test_memory_store():
    store = MemoryKeyStore()
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
    store.set_batch(range(10), range(10))
    assert len(store) == 10
    assert list(store.get_batch(range(10))) == list(range(10))


def test_memory_string_store():
    store = MemoryStringStore()
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

    assert set(store._store.keys()) == set(['test', 'another'])
    assert {key: store._store[key] for key in store._store.keys()} == {key: store.get(key) for key in store.keys()}

    store._flush()
    store.set_batch(range(20), range(20))
    assert len(store) == 20
    assert list(store.get_batch(range(20))) == [str(i) for i in range(20)]
