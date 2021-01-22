# Решение к домашней работе "Kafka"

## Задача
Напишете приложение для чтения данных из Kafka
Цель: В этом ДЗ научимся использовать Apache Kafka - управлять топиками и читать/писать данные с помощью Scala.
Перед началом выполнения требуется развернуть Kafka через docker-compose (https://github.com/Gorini4/kafka_scala_example) и создать топик books с 3 партициями.
Требуется написать приложение, которое будет выполнять следующее:
1. Вычитывать из CSV-файла, который можно скачать по ссылке - https://www.kaggle.com/sootersaalu/amazon-top-50-bestselling-books-2009-2019, данные, сериализовывать их в JSON, и записывать в топик books локльно развернутого сервиса Apache Kafka.
2. Вычитать из топика books данные и распечатать в stdout последние 5 записей (c максимальным значением offset) из каждой партиции. При чтении топика одновременно можно хранить в памяти только 15 записей.

## Решение

### Kafka Writer
1. Read from `csv` file provided as an argument
2. For each record in input file:
    1. convert `csv` to `json`
    2. write `json` to `kafka:books`

### Kafka Reader
1. `kafka:books`
2. create three queues of size 5 each
3. for each row in a records set dequeue an earlier row enqueue each next row

```scala
import scala.collection.mutable.Queue

new Queue(initialSize: Int = ArrayDeque.DefaultInitialSize)

def dequeue(): A
Removes the first element from this queue and returns it

def enqueue(elem: A): Queue.this.type
Add elements to the end of this queue
```