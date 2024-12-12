# SeamDB
SeamDB aims to be a computation cluster to seam existing services to provide database interfaces. Personnally, I treat it as LevelDB in distributed fashion.

## Give it a try

```
docker run --rm -p 5432:5432 -d kezhuw/seamdb

docker run -it --rm jbergknoff/postgresql-client postgresql://host.docker.internal/db1

db1=> CREATE DATABASE db1;
db1=> CREATE TABLE table1 (id serial PRIMARY KEY, count bigint NOT NULL, price real NOT NULL, description text);
db1=> INSERT INTO table1 (count, price, description) VALUES (4, 15.6, NULL), (3, 7.8, 'NNNNNN'), (8, 3.4, 'a'), (8, 2.9, 'b');
db1=> SELECT id, count, price description FROM table1 ORDER BY count DESC, id ASC;
db1=> SELECT sum(count) AS count, max(price) AS max_price, min(price) AS min_price, sum(count*price) AS sales_amount from table1 ORDER BY max(price) DESC;
```

## Seaming services
* Etcd or alikes as bootstrap cluster meta. It serves `CURRENT` as in LevelDB.
* Kafka or alikes as manifest log and data log. It serves `MANIFEST` and `LOG` as in LevelDB.
* HDFS or alikes as file backend. It servers `SST` as in LevelDB.

## Database interfaces
KV and then SQL.
