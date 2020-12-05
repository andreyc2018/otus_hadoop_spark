import scala.io.Source
import play.api.libs.functional.syntax._
import play.api.libs.json._

object CountriesApp extends App {
//  case class Country(name: String, capital: String, area: Int, region: String)
//  object Country {
//    implicit val countryReads: Reads[Country] = {
//      (JsPath \ "name")
//    }
//  }

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

  def test_source = Source.fromFile(
    "/home/andrey/Projects/learning/otus/otus_hadoop_spark/lesson_2/home_work/data/test.json"
  )
  case class FileName(size: String, datecreated: String, id: String, contenttype: String, filename: String)

  implicit val test_reads: Reads[FileName] = (
    (__ \ "size").read[String] and
      (__ \ "date-created").read[String] and
      (__ \ "id").read[String] and
      (__ \ "content-type" \ 0).read[String] and
      (__ \ "filename" \ "common").read[String]
    )(FileName)

  val test_json: JsValue = Json.parse(test_source.mkString(""))

  val nameResult = test_json.validate[List[FileName]]
//  println(nameResult)

  val list = nameResult match {
    case JsSuccess(list : List[FileName], _) => list
    case e: JsError => {
      println("Errors: " + JsError.toFlatForm(e).toString())
      List()
    }
  }

  println(list.filter(_.filename contains "morrow"))

//  val json: JsValue = Json.parse(source.mkString(""))
//  println(json.as[List])
//  println(List(json).take(2))
//  val json_a = Json.parse(source.mkString).as[JsArray]

//  println(List(json_a))
//  val name = (json \ "name")
//  val name = (json \ "name" \ "common").get.toString()
//  println(name)
}
