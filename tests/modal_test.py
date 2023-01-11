import sys
from spoonbill.datastores import ModalStore
import modal

image = modal.Image.debian_slim().pip_install("spoonbill-framework")
stub = modal.Stub("example-hello-world")
name = "test"
store = ModalStore.open(stub=stub, name=name)


@stub.function(image=image)
def modal_test(*args, **kwargs):
    store = ModalStore.open(name=name)
    store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.values(keys=['test'])) == ['test']
    assert list(store.items(conditions='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store['test'] = {'a': 1, 'b': 2}
    assert list(store.items(conditions={'a': 1})) == [('test', {'a': 1, 'b': 2})]
    store['function'] = lambda x: x + 1
    assert store['function'](1) == 2
    print("All tests passed!")


if __name__ == "__main__":
    with stub.run() as app:
        store = ModalStore.open(name=name, app=app)
        store['test'] = 'test'
        assert len(store) == 1
        store._flush()
        modal_test.call()
