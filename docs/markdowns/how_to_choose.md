# How to choose a backend?

For fastest performance, use the InMemoryStore. It is a simple dict that is not persisted to disk.      
If you need local persistence, use LmdbStore or ShelveStore. They are both fast and efficient.   
The JsonStore is another option with the benefit of a readable file, but it is slower - fitting for configurations.   


If speed is not important, but you want cheap persistence in the cloud, use FilesystemStore with S3,GCP, or Azure.

If you are using it to load tensors for embedding or deep learning weights, use SafetensorsStore

If you need persistence in the cloud with realtime search, use one of the Providers key-values store:

* CosmosDB (Azure)
* Firestore (GCP)
* DynamoDB (AWS)
* MongoDB (Wherever it is deployed)
* modal.Dict (Modal's own key-value store)

If you need very fast realtime, then the RedisStore is the best choice.

# Benchmarks
[Benchmarking LevelDB vs. RocksDB vs. HyperLevelDB vs. LMDB Performance for InfluxDB](https://www.influxdata.com/blog/benchmarking-leveldb-vs-rocksdb-vs-hyperleveldb-vs-lmdb-performance-for-influxdb/) By [Paul Dix](https://www.influxdata.com/blog/author/pauld)/Jun 20, 2014/[InfluxDB](https://www.influxdata.com/blog/category/tech/influxdb)
![Alt Text](https://w2.influxdata.com/wp-content/uploads/grid.png) 

## Conclusion 
LevelDB is the winner on disk space utilization, RocksDB is the winner on reads and deletes, and HyperLevelDB is the winner on writes. On smaller runs (30M or less), LMDB came out on top on most of the metrics except for disk size. This is actually what we’d expect for B-trees: they’re faster the fewer keys you have in them.

I’ve marked the LMDB compaction time as a loser in red because it’s a no-op and deletes don’t actually reclaim disk space. On a normal database where you’re continually writing data, this is ok because the old pages get used up. However, it means that the DB will ONLY increase in size. For InfluxDB this is a problem because we create a separate database per time range, which we call a shard. This means that after a time range has passed, it probably won’t be getting any more writes. If we do a delete, we need some form of compaction to reclaim the disk space.

On disk space utilization, it’s no surprise that the Level variants came out on top. They compress the data in blocks while LMDB doesn’t use compression.

Overall it looks like RocksDB might be the best choice for our use case. However, there are lies, damn lies, and benchmarks. Things can change drastically based on hardware configuration and settings on the storage engines.

