# How to choose a backend?

For fastest performance, use the InMemoryStore. It is a simple dict that is not persisted to disk.      
If you need local persistence, use Lmdb store or ShelveStore. They are both fast and efficient.

If speed is not important, but you want cheap persistence in the cloud, use FilesystemStore with S3,GCP, or Azure.

If you are using it to load tensors for embedding or deep learning weights, use SafetensorsStore

If you need persistence in the cloud with realtime search, use one of the Providers key-values store:

* CosmosDB (Azure)
* Firestore (GCP)
* DynamoDB (AWS)
* MongoDB (Wherever it is deployed)
* modal.Dict (Modal's own key-value store)

If you need very fast realtime, then the RedisStore is the best choice.   