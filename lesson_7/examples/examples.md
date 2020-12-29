## Metadata for all topics
```sh
kafkacat -b localhost:29092 -L
```

## Read a topic
```sh
kafkacat -b localhost:29092 -t mytopic
```

## Create a topic
```sh
kafka-topics --zookeeper localhost:2181 --create --topic mytopic --partitions 1 --replication-factor 1
```

## Write to a topic
`-P` - producer
```sh
kafkacat -P -b localhost:29092 -t mytopic
```
