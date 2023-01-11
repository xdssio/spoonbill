import sys
from spoonbill.datastores import ModalStore
import modal

image = modal.Image.debian_slim().pip_install("spoonbill-framework")
stub = modal.Stub("example-hello-world")
store = ModalStore(stub)


@stub.function(image=image)
def f(i):
    if i % 2 == 0:
        print("hello", i)
    else:
        print("world", i, file=sys.stderr)

    return i * i


if __name__ == "__main__":
    with stub.run() as app:
        store.set_context(app)
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
        print(store)
        assert len(store) == 1

        assert store.update({'test': 'test2', 'another': 'another2'})
        assert len(store) == 2
        assert store == {'test': 'test2', 'another': 'another2'}

        store['test'] = {'a': 1, 'b': 2}
        assert list(store.items(conditions={'a': 1})) == [('test', {'a': 1, 'b': 2})]
        store['function'] = lambda x: x + 1
        assert store['function'](1) == 2
