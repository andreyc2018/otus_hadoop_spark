package edu.lesson_7.kafka

import play.api.libs.json.{JsNumber, JsString}

class KafkaWriter(filename: String) {
  printf("Will read CSV file \"%s\"\n", filename)
  private val bufferedSource = io.Source.fromFile(filename)

  def write() {
    var total = 0
    var index = 0
    for (line <- bufferedSource.getLines) {
      if (index > 0) {
        println(s"line: $line")
        //    (?:,|\n|^)("(?:(?:"")*[^"]*)*"|[^",\n]*|(?:\n|$))
        //    val result = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))")
        //    result.foreach(f => println(f))
        val Array(name, author, rating, reviews, price, year, genre) = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").map(_.trim)
        //      val book = Book(name, author, rating, reviews, price, year, genre)
        //      println(s"array: $name $author $rating $reviews $price $year $genre")
        //      println(book)
        val m = Map("name" -> JsString(name), "author" -> JsString(author),
          "rating" -> JsNumber(rating.toDouble), "reviews" -> JsNumber(reviews.toInt),
          "price" -> JsNumber(price.toDouble), "year" -> JsNumber(year.toDouble), "genre" -> JsString(genre))
        println(s"m = $m")
      }
      index += 1
      total += 1
    }
    println(s"total = $total")
  }
}
