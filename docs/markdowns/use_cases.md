# Use cases

## Testing and prototyping.

When implementing logic using a cloud provider backend. It is often useful to have mock data for testing.    
Using InMemoryStore for mock data is a good way to test the logic without having to use a cloud provider backend or
running infrastructure locally.

Example:

```python
from spoonbill.datastores import DynamoDBStore, InMemoryStore

import os

environment = os.getenv("environment", "test")

if environment == "test":
    store = InMemoryStore.open("mock data")
elif environment == "dev":
    store = DynamoDBStore.open("dev table")
else:
    store = DynamoDBStore.open("prod table")
```

## Data science with cloud infrastructure.

Simple Example

```python
import numpy as np
import pandas as pd
from spoonbill.datastores import DynamoDBStore

df = pd.DataFrame({'user': [1, 2, 3]})
feature_store = DynamoDBStore.open("features table")  # {1: {"age":20:, "sex":female",...}}


def get_user_details(x):
    default = {"age": 25, "sex": "female"}
    return pd.Series(feature_store.get(x['user'], default).values())


df[['age', 'sex']] = df.apply(get_user_details, axis=1)
"""
   user  age     sex
0     1   20    male
1     2   30  female
2     3   25  female
"""
```

## Online machine learning

```python
from spoonbill.datastores import RedisStore, FilesystemStore
import time


class Model:
  def __init__(self):
    self.version = 123
    self.versions = RedisStore.open("redis://model_versions")
    self.models = FilesystemStore.open("s3://models/")  # can also use a faster store if needed
    self._model = self.models.get(self.version)

  @property
  def model(self):
    if self.version != self.versions.get("version"):
      self.version = self.versions.get("version")
      self._model = self.models.get(self.version)
    return self._model

  def partial_fit(self, x, y):
    """
    Fit a single example
    * Best to run as a single process from a queue 
    """
    new_model = self._model.partial_fit(x, y)
    new_model_id = int(time.time())
    self.models[new_model_id] = new_model
    self.versions["version"] = new_model_id

  # can scale horizontally by adding more workers 
  def predict(self, x):
    return self.model.predict(x)

```

### Advance machine learning example

Let's say we have a model that predicts the probability of a user watching a video based on their history.

* We train a user-video embeddings every day.
* We update the model every hour.
* The last 3 movies are updated in real-time.

```python
import numpy as np
from datetime import datetime as dt
from spoonbill.datastores import RedisStore, SafetensorsStore, FilesystemStore

# Whenever a user watches a video, we update the last 3 movies list.
recently_watched_store = RedisStore.open("redis://last_3_movies")  # {1: [1, 2, 3]}
video_embedding = SafetensorsStore.open("video_embedding.db")  # {1: [0.1, 0.2, 0.3]}
models = FilesystemStore.open("s3://models/22-02-2022/")  # {"v1": SuperNN(),...}

model = models.get(dt.now().hour, "default_model")  # update the model every hour
video_embedding.load('s3://video_embeddings/day')  # load the video embeddings every day


def get_user_embedding(user):
  # online feature engineering 
  default_embedding, most_popular_movies = [0.1, 0.1, 0.1], [1, 2, 3]
  last_3_movies = recently_watched_store.get(user, most_popular_movies)
  return np.mean([video_embedding.get(movie, default_embedding) for movie in last_3_movies], axis=0)


def predict(user):
  return model.predict(get_user_embedding.get(user))

```

* This can be even more efficient if we save the already computed embeddings straight in redis, but this gets the point
  across.



