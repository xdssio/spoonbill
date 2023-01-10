from spoonbill.datastores import ContextStore
import modal


class ModalStore(ContextStore):

    def __init__(self, name: str, **kwargs):
        self.stub = modal.Stub(name, **kwargs)
        self.stub.main = modal.Dict()
        self._store = self.stub.main
        self.name = name

    @property
    def context(self):
        return self.stub.run()

    def __getitem__(self, item):
        with self.context as store:
            return store.main[item]

    def __setitem__(self, key, value):
        with self.context as store:
            store.main[key] = value

    def __contains__(self, item):
        with self.context as store:
            return item in store.main

    def __len__(self):
        with self.context as store:
            return len(store.main)
