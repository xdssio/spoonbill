import sys
from spoonbill.datastores import ModalStore
import modal

image = modal.Image.debian_slim().pip_install("spoonbill-framework")
stub = modal.Stub("example-hello-world")
store = ModalStore(stub, data={"hello": "world"})
stub.main = modal.Dict(data={"s": set([1, 2])})


@stub.function(image=image)
def f(i):
    if i % 2 == 0:
        print("hello", i)
    else:
        print("world", i, file=sys.stderr)

    return i * i


if __name__ == "__main__":
    with stub.run() as app:
        store.set_store(app)
        print(len(store), store._keys)
        for i in range(3):
            store[i] = f(i)
        print(store)
        # Call the function directly.
        # store.set_store(app)
        # key = 4
        # store[key] = f.call(key)
        # print(store)

        # Parallel map.
        # total = 0
        # for ret in f.map(range(10)):
        #     total += ret
        #
        # print(total)
