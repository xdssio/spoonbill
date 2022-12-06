# Spoonbill

What is Spoonbill?

Spoonbill is a Python library that provides a lightweight, universal interface for ML data stores. It helps Python users
explore and transform data of any size, stored anywhere.

```python
import spoonbill as sp

df = sp.open('titanic.parquet')
features_df = sp.open('titanic_features.parquet')
df = sp.join(df, features_df, on='PassengerId')
features_dict = sp.KeyValueStore('titanic_features.parquet')
sp.join(df, features_dict, on='PassengerId')



```
