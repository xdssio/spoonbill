# Quickstart

## Installation

`pip install spoonbill-framework`

## Usage

```python
from spoonbill.datastores import InMemoryStore

store = InMemoryStore()  # InMemoryDict.open() or InMemoryDict.open('path/to/file') from file
store["key"] = "value"
store["key"] = {"feature": "value"}
store["key"] == "value"
del store['key']
store.set("key", "value")
store.get("key", None)
store.delete("key")
store.pop('key', None)
store.popitem()
store.keys()
store.items()
store.values()
'key' in store  # contains
len(store)
for key in store: pass  # iterate
store.update({'key': 'value'})
store.save("path/to/file")  # works with cloud locations too
store.load("path/to/file")
```

**Searches** are supported when using `strict=True`.

```python
# scan keys to a pattern
store.keys(pattern="*", limit=10)
# retrieve a batch of values efficiently
store.values(keys=['key1', 'key2'])
# filter based on match conditions
store.items(conditions={'a': '1', 'b': 1}, limit=10)

```

MongoDB example:

```python   
from spoonbill.datastors import MongoDBStore

store = MongoDBStore(uri="mongodb://...", strict=True)
store["key1"] = {"a":'1', "b":2}
store["key2"] = {"a":'1', "b":3}
store.keys(pattern="*", limit=10) 
store.values(keys=['key1', 'key2']) 
store.items(conditions={'a': '1', 'b': 1}, limit=10)

```

When using `strict=False` we can save an object with any key, but we lose the ability to search.

```python
from spoonbill.datastores import RedisStore

store = RedisStore(strict=False)
store['function'] = lambda x: x + 1
store['function'](1) == 2
```