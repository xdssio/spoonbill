from spoonbill import RedisDict


def test_redis_from_connection():
    store = RedisDict.from_connection('localhost', 6379, 1)
    store[1] = 1
    assert store[1] == 1
    store._flush()


def test_redis_from_url():
    store = RedisDict.from_url('redis://localhost:6379/1')
    store[1] = 1
    assert store[1] == 1
    store._flush()


def test_redis_dict():
    store = RedisDict.from_url('redis://localhost:6379/1')
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
    store['function'] = lambda x: x
    assert store['function'](1) == 1

    store._flush()
    store.update({'1': 1, '11': 11, '2': 2, '3': 3, 1: -1, 11: -11})
    assert set(store.scan('1*')) == set(store.keys('1*')) == set(['1', '11'])  # scan only works with string keys
    store._flush()


def test_redis_as_string():
    store = RedisDict.from_url('redis://localhost:6379/1', as_strings=True)
    store._flush()
    store[1] = 1
    assert '1' in store
    assert store[1] == '1'
    assert store.get(1) == '1'
    assert store.get('1') == '1'
    assert store[1] == store._store[1] == store['1'] == store._store['1']
    store[11] = 11
    store[2] = 2
    assert set(store.scan('1*')) == set(store.keys('1*')) == set(['1', '11'])
