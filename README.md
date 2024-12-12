# SeamDB
SeamDB aims to be a computation cluster to seam existing services to provide database interfaces. Personnally, I treat it as LevelDB in distributed fashion.

## Give it a try

```bash
docker run --rm -p 5432:5432 kezhuw/seamdb
```

Wait few seconds for seamdb to up.
```bash
docker run -it --rm jbergknoff/postgresql-client postgresql://host.docker.internal/db1
```

```sql
CREATE DATABASE db1;
```

```sql
CREATE TABLE table1 (id serial PRIMARY KEY, count bigint NOT NULL, price real NOT NULL, description text);
```

```sql
INSERT INTO table1 (count, price, description) VALUES (4, 15.6, NULL), (3, 7.8, 'NNNNNN'), (8, 3.4, 'a'), (8, 2.9, 'b');
```

```sql
SELECT id, count, price description FROM table1 ORDER BY count DESC, id ASC;
```

```sql
SELECT sum(count) AS count, max(price) AS max_price, min(price) AS min_price, sum(count*price) AS sales_amount from table1 ORDER BY max(price) DESC;
```

## Seaming services
* Etcd or alikes as bootstrap cluster meta. It serves `CURRENT` as in LevelDB.
* Kafka or alikes as manifest log and data log. It serves `MANIFEST` and `LOG` as in LevelDB.
* HDFS or alikes as file backend. It servers `SST` as in LevelDB.

## Database interfaces
KV and then SQL.
