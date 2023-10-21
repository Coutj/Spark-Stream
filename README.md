# Criar topico

```bash
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic test
```

# Listar topicos

```bash
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092
```

# Producer

```bash
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic test
```

# Consumer
```bash
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic test \
    --from-beginning
```

# Executando código
```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_stream_test.py
```