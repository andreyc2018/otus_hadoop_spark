package edu.lesson_7.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object Reader {

  def readFrom(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put("group.id", "lesson_7_reader")
    val consumer = new KafkaConsumer(props, new LongDeserializer, new StringDeserializer)

    println(s"Subscribing to $topic")
    consumer.subscribe(List(topic).asJavaCollection)

    println(s"Reading from $topic")
    consumer
      .poll(Duration.ofSeconds(3))
      .asScala
      .foreach({ r => printf("%s: %s\n", r.key(), r.value()) })

    consumer.close()
    println(s"Done reading from $topic")
  }
}
