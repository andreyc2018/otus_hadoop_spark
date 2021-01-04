package edu.lesson_7.kafka

import play.api.libs.json.{JsNumber, JsString, JsValue, Json}

class KafkaWriter(file: String) {
  printf("Will read CSV file \"%s\"\n", file)
  private val filename: String = file

  def lineToMap(line: String): Map[String, JsValue with Serializable] = {
    val Array(name, author, rating, reviews, price, year, genre) = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").map(_.trim)
    Map("name" -> JsString(name), "author" -> JsString(author),
      "rating" -> JsNumber(rating.toDouble), "reviews" -> JsNumber(reviews.toInt),
      "price" -> JsNumber(price.toDouble), "year" -> JsNumber(year.toDouble), "genre" -> JsString(genre))
  }

  def write(): Unit = {
    val bufferedSource = io.Source.fromFile(filename)
    bufferedSource.getLines().drop(1).foreach(line => {
      val m = lineToMap(line)
      println(s"m = $m")
      val dataToWrite: JsValue = Json.toJson(m)
      println(s"json: $dataToWrite")
    })

    bufferedSource.close()
  }
}
