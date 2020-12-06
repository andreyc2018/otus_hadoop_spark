import scala.io.Source
import play.api.libs.functional.syntax._
import play.api.libs.json._

import java.io.PrintWriter
import scala.collection.immutable

object CountriesApp extends App {
//  def source = Source.fromURL(
//    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
//  )

  def source = Source.fromFile(
    "/home/andrey/Projects/learning/otus/otus_hadoop_spark/lesson_2/home_work/data/countries_short.json"
  )
  case class Country(name: String, capital: String, region: String, area: Int)

  implicit val reads: Reads[Country] = (
    (__ \ "name" \ "common").read[String] and
      (__ \ "capital" \ 0).read[String] and
      (__ \ "region").read[String] and
      (__ \ "area").read[Int]
  )(Country)

  val countriesJson: JsValue = Json.parse(source.mkString(""))

  val countriesValidated: JsResult[List[Country]] = countriesJson.validate[List[Country]]

  val countriesList: immutable.Seq[Country] = countriesValidated match {
    case JsSuccess(list: List[Country], _) => list
    case e: JsError => { List() }
  }

  def filterRecords(region: String)(countries: Iterable[Country]): Iterable[Country] = {
    countries.filter(country => (country.region == region))
  }

  def renderRecord()(country: Country) = {
//    { "name": "bla", "capital: "bar", "area": 100 }

  }

  def writeToFile(fileName: String, region: String, count: Int) = {
    val dataToWrite =
      filterRecords(region)(countriesList)
        .map(renderRecord())
        .mkString("\n")

    val writer = new PrintWriter(fileName)
    writer.println(dataToWrite)
    writer.flush()
    writer.close()
  }

  println(countriesList.filter(_.region contains "Africa"))
}
