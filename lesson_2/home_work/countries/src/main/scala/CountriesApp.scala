import scala.io.Source
import play.api.libs.functional.syntax._
import play.api.libs.json._

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

  val countriesJson = Json.parse(source.mkString(""))

  val countriesValidated = countriesJson.validate[List[Country]]

  val countriesList = countriesValidated match {
    case JsSuccess(list: List[Country], _) => list
    case e: JsError => {
      List()
    }
  }

  println(countriesList.filter(_.region contains "Africa"))
}
