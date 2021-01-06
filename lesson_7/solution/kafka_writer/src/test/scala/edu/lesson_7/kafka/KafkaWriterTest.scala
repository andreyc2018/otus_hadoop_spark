package edu.lesson_7.kafka

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.{JsValue, Json}

class KafkaWriterTest extends AnyFunSuite with BeforeAndAfterEach {

  test("testLineToMap") {
    val data = "Name,Author,User Rating,Reviews,Price,Year,Genre\n1984 (Signet Classics),George Orwell,4.7,21424,6,2017,Fiction\n\"5,000 Awesome Facts (About Everything!) (National Geographic Kids)\",National Geographic Kids,4.8,7665,12,2019,Non Fiction\n"
    val expectedObjects = Vector(
      Vector(
        Map("author" -> "George Orwell", "name" -> "1984 (Signet Classics)", "genre" -> "Fiction"),
        Map("price" -> 6, "year" -> 2017, "rating" -> 4.7, "reviews" -> 21424)
      ),
      Vector(
        Map("author" -> "National Geographic Kids", "name" -> "\"5,000 Awesome Facts (About Everything!) (National Geographic Kids)\"", "genre" -> "Non Fiction"),
        Map("price" -> 12, "year" -> 2019, "rating" -> 4.8, "reviews" -> 7665)
      )
    )
//      "{ \"name\": \"\", \"author\": \"\", \"rating\": 0, \"reviews\": 0, \"price\": 0, \"year\": 1234, \"genre\": \"\" }",
//      "{ \"name\": \"\", \"author\": \"\", \"rating\": 0, \"reviews\": 0, \"price\": 0, \"year\": 1234, \"genre\": \"\" }")
    val bufferedSource = io.Source.fromString(data)
    val writer: KafkaWriter = new KafkaWriter("inputFile")

    println(expectedObjects)

    var idx = 0
    for (line <- bufferedSource.getLines().drop(1)) {
      val dataToWrite = writer.lineToJson(line)
      println(s"json: $dataToWrite")
      val expected = expectedObjects(idx)
      println(expected(0))
      for ((k,v) <- expected(0)) {
        val a = (dataToWrite \ k).as[String]
        println(s"k = $k, v = $v, a = $a")
        assert(v == a)
      }
      for ((k,v) <- expected(1)) {
        val a = (dataToWrite \ k).as[Double]
        println(s"k = $k, v = $v, a = $a")
        assert(v == a)
      }
      idx += 1
    }
  }

}