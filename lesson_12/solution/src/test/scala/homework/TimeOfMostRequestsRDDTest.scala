package homework

import homework.DataApiHomeWorkTaxi.readStats
import org.apache.spark.sql.SparkSession

import org.scalatest.flatspec.AnyFlatSpec

class TimeOfMostRequestsRDDTest extends AnyFlatSpec {

  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Test №1: TimeOfMostRequests")
    .getOrCreate()

  it should "upload and find time of most requests" in {
    val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
    println(s"reading from $taxiFactsFile")
    val taxiFactsDF = readStats(taxiFactsFile, spark)

    val dataToWrite = TimeOfMostRequestsRDD.process(taxiFactsDF.rdd)
    val actualData = dataToWrite.collect()
    assert(!actualData.isEmpty)
    assert(actualData.head == "11 - 12 : 22121")
    assert(actualData.last == "21 - 22 : 1586")
  }
}
