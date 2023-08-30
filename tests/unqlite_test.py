from tempfile import TemporaryDirectory

from spoonbill.datastores.unqlite import UnQLiteStore


def test_unqlite_strict():
    store = UnQLiteStore.open(strict=True)
    store['test'] = b'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == b'test'
    store.set('another', b'another')
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set([b'test', b'another'])
    assert set(store.items()) == set([('test', b'test'), ('another', b'another')])

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.items(conditions=b'test')) == [('test', b'test')]

    assert store.pop('another') == b'another'
    assert len(store) == 1

    assert store.update({'test': b'test2', 'another': b'another2'})
    assert len(store) == 2
    assert store == {'test': b'test2', 'another': b'another2'}


def test_unqlite_nonstrict():
    store = UnQLiteStore.open(strict=False)
    store.update({i: i for i in range(11)})

    assert len(store) == 11
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 11  # test iterator

    store = UnQLiteStore.open(strict=True)
    store.update({i: i for i in range(11)})
    assert [item for item in store.keys(pattern='1+')] == ['1', '10']  # scan looks at keys as strings


# TODO with db.collection
def test_unqlite_search():
    # problem at self._is_encoded
    store = UnQLiteStore.open(strict=False)
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='1+')) == {1, '18', '10', '12', '15', '14', '16', '17', '19', '13', '11', '1'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [{'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]


def test_unqlite_persistence():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = UnQLiteStore.open(path)
    store['test'] = 'test'
    assert len(store) == 1

    store = UnQLiteStore.load(path)  # equals to open
    assert len(store) == 1
