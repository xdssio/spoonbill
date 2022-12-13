from spoonbill import InMemoryDict


def test_dict():
    store = InMemoryDict()
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

    assert list(store.scan('1*')) == list(range(10))  # scan looks at keys as strings
