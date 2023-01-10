import sys
from spoonbill.datastores import ModalStore
import modal

image = modal.Image.debian_slim().pip_install("spoonbill-framework")

store = ModalStore("example-hello-world")
stub = store.stub


@stub.function(image=image)
def f(i):
    if i % 2 == 0:
        print("hello", i)
    else:
        print("world", i, file=sys.stderr)

    return i * i


if __name__ == "__main__":
    with stub.run():
        # Call the function directly.
        key = 4
        store['key'] = f.call(key)
        print(store['key'])

        # Parallel map.
        total = 0
        for ret in f.map(range(10)):
            total += ret

        print(total)
