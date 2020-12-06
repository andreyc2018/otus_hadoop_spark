import scala.io.Source
import play.api.libs.functional.syntax._
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json._

import java.io.PrintWriter
import scala.collection.immutable

object CountriesApp extends App {
  def source = Source.fromURL(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
  )

  case class Country(name: String, capital: String, region: String, area: Float)

  implicit val reads: Reads[Country] = (
    (__ \ "name" \ "common").read[String] and
      (__ \ "capital" \ 0).read[String].orElse(Reads.pure("")) and
      (__ \ "region").read[String] and
      (__ \ "area").read[Float]
  )(Country)

  val countriesJson: JsValue = Json.parse(source.mkString(""))

  val countriesValidated: JsResult[List[Country]] = countriesJson.validate[List[Country]]

  val countriesList: immutable.Seq[Country] = countriesValidated match {
    case JsSuccess(list: List[Country], _) => list
    case e: JsError => {
      println(e)
      List()
    }
  }

  def filterRecords(region: String, count: Int)(countries: Seq[Country]) = {
    countries.filter(country => (country.region == region)).sortBy(- _.area).take(count)
  }

  // { "name": "bla", "capital: "bar", "area": 100 }
  def convertCountiesToJson(countries: Seq[Country]): JsValue = {
    Json.toJson(
      countries.map { c =>
        Map("name" -> JsString(c.name), "capital" -> JsString(c.capital), "area" -> JsNumber(c.area))
      }
    )
  }

  def writeToFile(fileName: String, region: String, count: Int) = {
    val countries: Seq[Country] = filterRecords(region, count)(countriesList)
    val dataToWrite: JsValue = convertCountiesToJson(countries)

    val writer = new PrintWriter(fileName)
    writer.println(Json.prettyPrint(dataToWrite))
    writer.flush()
    writer.close()
  }

  val outFile = args(0)

  writeToFile(outFile, "Africa", 10)
}
