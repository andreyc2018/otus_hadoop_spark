package edu.lesson_7.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ListBuffer, Map, Queue}

object Reader {
  case class Key(showId: String)

  private def printRecord(record: BookRecord): Unit = {
    println(s"${record.offset}: ${record.data}")
  }

  private def printPartition(partition: Queue[BookRecord]): Unit = {
    partition.foreach(r => printRecord(r))
  }

  def readFrom(topic: String): Unit = {
    val props = new Properties()
    val batchSize = 5
    val keepSize = 3
    props.put("bootstrap.servers", "localhost:29092")
    props.put("group.id", "lesson_7_reader")
    props.put("max.poll.records", batchSize)
    val consumer = new KafkaConsumer(props, new LongDeserializer, new StringDeserializer)

//    val booksPartitions: Vector[TopicPartition] = consumer
//      .partitionsFor("books")
//      .asScala
//      .toVector
//      .map(info => new TopicPartition(info.topic(), info.partition()))
//
//    val offset: lang.Long = 5
//
//    println(s"parts = $booksPartitions")
//    consumer.assign(booksPartitions.asJava)
//    consumer.seekToEnd(consumer.assignment())
//    consumer.seek(booksPartitions.asJava, offset)
//    val records: ConsumerRecords[lang.Long, String] = consumer.poll(Duration.ofSeconds(3))
//    println(s"Just polled the ${records.count()} books.")

    println(s"Subscribing to $topic")
    consumer.subscribe(List(topic).asJavaCollection)

    println(s"Reading records in batches of $batchSize records")
    val offsets = Map[Int, ListBuffer[Long]]()
    offsets(0) = ListBuffer[Long]()
    offsets(1) = ListBuffer[Long]()
    offsets(2) = ListBuffer[Long]()
    val keepRecords = Map[Int, Queue[BookRecord]]()
    var readRecords = 0
    var count = 0
    do {
      val records = consumer.poll(Duration.ofSeconds(5))
      println(s"Read ${records.count()} records")
      count = records.count()
      readRecords += count
      for (record <- records.asScala) {
        if (!keepRecords.contains(record.partition())) {
          keepRecords(record.partition()) = Queue[BookRecord]()
        }
        if (keepRecords(record.partition()).size == keepSize) {
          keepRecords(record.partition()).dequeue()
        }
        keepRecords(record.partition()).enqueue(BookRecord(record.offset(), record.value()))
        offsets(record.partition()) += record.offset()
        println(
          s"""topic = ${record.topic()}, partition = ${record.partition()}, offset = ${record.offset()}
             |	key = ${record.key()}, data = ${record.value()}""".stripMargin)
      }
    } while (count > 0)
    println(s"Done reading ${readRecords} records from $topic")
    offsets.foreach(partition => { println(s"$partition") })
    keepRecords.foreach(partition => {
      println(s"${partition._1}")
      printPartition(partition._2)
    })
    consumer.close()

//    println(s"Reading from $topic")
//    val p1 = new TopicPartition(topic, 0)
//    val p2 = new TopicPartition(topic, 1)
//    val p3 = new TopicPartition(topic, 2)
//    val partitions = List(p1, p2, p3)
//    println(s"partitions= $partitions")
//    consumer.assign(partitions.asJavaCollection)
//    consumer.seekToEnd(partitions.asJavaCollection)
//    val e1 = consumer.position(p1)
//    println(s"e1 = $e1")
//    val e2 = consumer.position(p2)
//    println(s"e2 = $e2")
//    val e3 = consumer.position(p3)
//    println(s"e3 = $e3")

//    val records = consumer.poll(Duration.ofSeconds(3))
//    for (record <- records.asScala) {
//      var counter = 0
//      println(s"record = $record")

//      log.debug("topic = %s, partition = %d, offset = %d,"
//        customer = %s, country = %s\n",
//                record.topic(), record.partition(), record.offset(),
//      record.key(), record.value());

//      int updatedCount = 1;
//      if (custCountryMap.countainsKey(record.value())) {
//        updatedCount = custCountryMap.get(record.value()) + 1
//      }
//      custCountryMap.put(record.value(), updatedCount)
//
//      JSONObject json = new JSONObject(custCountryMap)
//      println(json.toString(4))
//    }

//    consumer
//      .poll(Duration.ofSeconds(3))
//      .asScala
//      .foreach({ r => printf("%s: %s\n", r.key(), r.value()) })

  }
}
