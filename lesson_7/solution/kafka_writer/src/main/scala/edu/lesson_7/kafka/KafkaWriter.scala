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
//    var total = 0
//    var index = 0
    bufferedSource.getLines().drop(1).foreach(line => {
      val m = lineToMap(line)
      println(s"m = $m")
      val dataToWrite: JsValue = Json.toJson(m)
      println(s"json: $dataToWrite")
    })

//    for (line <- bufferedSource.getLines) {
//      if (index > 0) {
//        println(s"line: $line")
//        //    (?:,|\n|^)("(?:(?:"")*[^"]*)*"|[^",\n]*|(?:\n|$))
//        //    val result = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))")
//        //    result.foreach(f => println(f))
//        val Array(name, author, rating, reviews, price, year, genre) = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").map(_.trim)
//        //      val book = Book(name, author, rating, reviews, price, year, genre)
//        //      println(s"array: $name $author $rating $reviews $price $year $genre")
//        //      println(book)
//        val m = Map("name" -> JsString(name), "author" -> JsString(author),
//          "rating" -> JsNumber(rating.toDouble), "reviews" -> JsNumber(reviews.toInt),
//          "price" -> JsNumber(price.toDouble), "year" -> JsNumber(year.toDouble), "genre" -> JsString(genre))
//        println(s"m = $m")
//      }
//      index += 1
//      total += 1
//    }
//    println(s"total = $total")
    bufferedSource.close()
  }
}
