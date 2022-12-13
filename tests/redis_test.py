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


def test_redis_open():
    store = RedisDict.open('redis://localhost:6379/1')
    store[1] = 1
    assert store[1] == 1
    store._flush()


def test_redis_dict():
    store = RedisDict.open('redis://localhost:6379/1')
    assert store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains
    store.set(1, 1)
    assert set(store.keys()) == set(['test', 'another', 1])
    assert set(store.values()) == set(['test', 'another', 1])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another'), (1, 1)])

    assert store.pop('another') == 'another'
    assert len(store) == 2

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 3
    assert store == {'test': 'test2', 'another': 'another2', 1: 1}

    store._flush()
    store.set_batch(range(10), range(10))
    assert len(store) == 10
    assert list(store.get_batch(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 10  # test iterator

    store['function'] = lambda x: x
    assert store['function'](1) == 1

    store._flush()
    store.update({'1': 1, '11': 11, '2': 2, '3': 3, 1: -1, 11: -11})
    assert set(store.scan('1*')) == set(store.keys('1*')) == set(['1', '11'])  # scan only works with string keys
    assert set(store.items()) == set([('1', 1), ('11', 11), ('2', 2), ('3', 3), (1, -1), (11, -11)])
    store._flush()


def test_redis_as_string():
    store = RedisDict.open('redis://localhost:6379/1', as_strings=True)
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
