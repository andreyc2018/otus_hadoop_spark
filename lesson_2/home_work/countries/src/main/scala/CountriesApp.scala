import scala.io.Source
import play.api.libs.functional.syntax._
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json.Reads.minLength
import play.api.libs.json._

import java.io.PrintWriter
import scala.collection.immutable

object CountriesApp extends App {
  def source = Source.fromURL(
    "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
  )

//  def source = Source.fromFile(
//    "/home/andrey/Projects/learning/otus/otus_hadoop_spark/lesson_2/home_work/data/countries_short.json"
//  )
  case class Country(name: String, capital: String, region: String, area: Float)

  implicit val reads: Reads[Country] = (
    (__ \ "name" \ "common").read[String] and
      (__ \ "capital" \ 0).read[String].orElse(Reads.pure("")) and
      (__ \ "region").read[String] and
      (__ \ "area").read[Float]
  )(Country)

  val countriesJson: JsValue = Json.using[Json.WithDefaultValues].parse(source.mkString(""))

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
    writer.println(dataToWrite)
    writer.flush()
    writer.close()
  }

  println(countriesList)
//  println(convertCountiesToJson(countriesList))
//  println(convertCountiesToJson(countriesList.filter(_.region contains "Africa").sortBy(- _.area).take(1)))

  val outFile = args(0)

  writeToFile(outFile, "Africa", 10)
//  val tweetsString =
//    """
//      |[
//      |    {"username":"John", "tweet":"Scala rules!", "date":"Mon Sep 23 07:38:13 MDT 2013"},
//      |    {"username":"Jane", "tweet":"Play is awesome!", "date":"Mon Sep 23 07:38:15 MDT 2013"},
//      |    {"username":"Fred", "tweet":"FP rocks!", "date":"Mon Sep 23 07:38:17 MDT 2013"}
//      |]
//      |""".stripMargin
//
//  case class Tweet(username: String, tweet: String, date: String)
//
//  implicit val tweetReads: Reads[Tweet] = (
//    (__ \ "username" ).read[String] and
//      (__ \ "tweet" ).read[String] and
//      (__ \ "date").read[String]
//    )(Tweet)
//
//  val tweetsJson: JsValue = Json.parse(tweetsString)
//
//  val tweetsValidated: JsResult[List[Tweet]] = tweetsJson.validate[List[Tweet]]
//
//  val tweetsList: immutable.Seq[Tweet] = tweetsValidated match {
//    case JsSuccess(list: List[Tweet], _) => list
//    case e: JsError => { List() }
//  }
//
//  def convertTweetsToJsonOrig(tweets: Seq[Tweet]): JsValue = {
//    Json.toJson(
//      tweets.map { t =>
//        Map("username" -> t.username, "tweet" -> t.tweet, "date" -> t.date)
//      }
//    )
//  }
//
//  def writeTweetsToFile(fileName: String, tweets: Seq[Tweet]) = {
//    val dataToWrite = convertTweetsToJsonOrig(tweets)
//    val writer = new PrintWriter(fileName)
//    writer.println(dataToWrite)
//    writer.flush()
//    writer.close()
//  }
//
//  println(tweetsList)
//  println(convertTweetsToJsonOrig(tweetsList))
//  writeTweetsToFile(outFile, tweetsList)
}
