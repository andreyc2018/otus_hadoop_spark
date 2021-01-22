package edu.lesson_7.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import java.time.Duration
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.collection.mutable.{Map, Queue}

object Reader {
  case class Key(showId: String)

  def readFrom(topic: String): mutable.Map[Int, mutable.Queue[BookRecord]] = {
    val props = new Properties()
    val batchSize = 15
    val keepSize = 5
    props.put("bootstrap.servers", "localhost:29092")
    props.put("group.id", "lesson_7_reader")
    props.put("max.poll.records", batchSize)
    val consumer = new KafkaConsumer(props, new LongDeserializer, new StringDeserializer)

    consumer.subscribe(List(topic).asJavaCollection)

    val keepRecords = Map[Int, Queue[BookRecord]]()
    var count = 0
    do {
      val records = consumer.poll(Duration.ofSeconds(5))
      count = records.count()
      for (record <- records.asScala) {
        if (!keepRecords.contains(record.partition())) {
          keepRecords(record.partition()) = Queue[BookRecord]()
        }
        if (keepRecords(record.partition()).size == keepSize) {
          keepRecords(record.partition()).dequeue()
        }
        keepRecords(record.partition()).enqueue(BookRecord(record.offset(), record.value()))
      }
    } while (count > 0)
    consumer.close()

    keepRecords
  }
}
