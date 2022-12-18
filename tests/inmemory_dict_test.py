from tempfile import TemporaryDirectory

from spoonbill.datastores import InMemoryDict


def test_dict_strict():
    store = InMemoryDict.open()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.items(pattern='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

def test_inmemory_dict():
    store = InMemoryDict(strict=False)
    store.set_batch(range(11), range(11))
    assert len(store) == 11
    assert list(store.get_batch(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 11  # test iterator

    store = InMemoryDict(strict=True)
    store.set_batch(range(11), range(11))
    assert [item[0] for item in store.scan(pattern='1+')] == [1, 10]  # scan looks at keys as strings


def test_inmemory_save_load():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = InMemoryDict.open()
    store['test'] = 'test'
    store.save(path)
    store._flush()
    assert len(store) == 0
    store = InMemoryDict().load(path)
    assert len(store) == 1
    store = InMemoryDict(path)


def test_inmemory_ordereddict():
    from collections import OrderedDict
    store = InMemoryDict.open(OrderedDict())
    store.update({key: key for key in range(1000)})
    assert list(store.keys()) == list(range(1000))
