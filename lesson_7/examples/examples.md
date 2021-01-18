## Metadata for all topics
```sh
kafkacat -b localhost:29092 -L
```

## Read a topic
```sh
kafkacat -b localhost:29092 -t books
```

## Topics commands
### List
```
kafka-topics --bootstrap-server localhost:29092 --list
```
### Create
```sh
kafka-topics --zookeeper localhost:2181 --create --topic mytopic --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:29092 --create --topic books --partitions 3
```
### Delete
```
kafka-topics --bootstrap-server localhost:29092 --delete --topic books
```
### Describe
```
kafka-topics --bootstrap-server localhost:29092 --describe --topic books
```

## Write to a topic
`-P` - producer
```sh
kafkacat -P -b localhost:29092 -t mytopic
```
