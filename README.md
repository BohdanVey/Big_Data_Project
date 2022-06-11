# Streaming of wikipedia events using Kafka #
Для того, щоб запустити програму спочатку потрібно налаштувати Cassandra 
та Kafka, це можна зробити за допомогою скриптів в папці scripts

```shell
bash scripts/run-cluster
bash scripts/run-cassandra
```

Після цього потрібно ініцілізувати бд, це можна зробити за допомогою файлу
populate_db/init_keyspace.cql по черзі виконавши всі команди в меню Cassandra

```shell
docker exec -it cassandra-node1 cqlsh
```


Після запуску всіх необхідних програм, можна запускати основний файл,
це можна зробити за допомогою Dockerfile і скриптy
```shell
bash build-and-run.py
```

Відповіді на precompute question будуть зберігатись в папці data
в форматі json.

В необхідний момент їх можна просто зчитати.

Відповіді на запити будуть оброблятись за допомогою Cassandra.

Для того щоб получити відповіді на запитання можна використовувати 
файл ad-hoc_responses/db_read.py

```shell
bash run_data_read.sh
```