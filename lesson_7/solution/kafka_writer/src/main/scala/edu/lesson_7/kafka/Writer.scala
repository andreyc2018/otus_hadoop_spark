package edu.lesson_7.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import play.api.libs.json.{JsNumber, JsString, JsValue, Json}

import java.time.Instant
import java.util.Properties

object Writer {

  def lineToJson(line: String): JsValue = {
    val Array(name, author, rating, reviews, price, year, genre) = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").map(_.trim)
    Json.toJson(Map("name" -> JsString(name), "author" -> JsString(author),
      "rating" -> JsNumber(rating.toDouble), "reviews" -> JsNumber(reviews.toInt),
      "price" -> JsNumber(price.toDouble), "year" -> JsNumber(year.toDouble), "genre" -> JsString(genre)))
  }

  def fromFile(filename: String, topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    val producer = new KafkaProducer(props, new LongSerializer, new StringSerializer)
    var counter = 0
    val bufferedSource = io.Source.fromFile(filename)
    bufferedSource.getLines().drop(1).foreach(line => {
      val dataToWrite = lineToJson(line)
      val data = Json.stringify(dataToWrite)
      val key = Instant.now.toEpochMilli + counter
      producer.send(new ProducerRecord(topic, key, data))
      println(s"sending key = ${key}")
      counter += 1
    })

    producer.close()
    bufferedSource.close()

    println(s"total sent ${counter} records")
  }
}