# SeamDB
SeamDB aims to be a computation cluster to seam existing services to provide database interfaces. Personnally, I treat it as LevelDB in distributed fashion.

## Seaming services
* Etcd or alikes as bootstrap cluster meta. It serves `CURRENT` as in LevelDB.
* Kafka or alikes as manifest log and data log. It serves `MANIFEST` and `LOG` as in LevelDB.
* HDFS or alikes as file backend. It servers `SST` as in LevelDB.

## Database interfaces
KV and then SQL.
