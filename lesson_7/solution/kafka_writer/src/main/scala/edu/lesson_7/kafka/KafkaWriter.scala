package edu.lesson_7.kafka

class KafkaWriter(filename: String) {
  printf("Will read CSV file \"%s\"\n", filename)
  private val bufferedSource = io.Source.fromFile(filename)

  def write() {
    for (line <- bufferedSource.getLines) {
      println(s"line: $line")
      //    (?:,|\n|^)("(?:(?:"")*[^"]*)*"|[^",\n]*|(?:\n|$))
      //    val result = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))")
      //    result.foreach(f => println(f))
      val Array(name, author, userRating, reviews, price, year, genre) = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").map(_.trim)
      val book = Book(name, author, userRating, reviews, price, year, genre)
      println(s"array: $name $author $userRating $reviews $price $year $genre")
      println(book)
    }
  }
}
