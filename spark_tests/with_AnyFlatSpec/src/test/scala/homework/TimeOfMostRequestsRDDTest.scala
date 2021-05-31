package homework

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class TimeOfMostRequestsRDDTest extends AnyFlatSpec with BeforeAndAfterAll {
  import ReadWriteUtils._
  import TimeOfMostRequests.processTaxiData

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("Test №1: TimeOfMostRequests")
      .getOrCreate()

    println(s"Created spark session $spark")

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    println(s"Closing spark session $spark")
    spark.close()
    super.afterAll()
  }

  it should "find time of most requests" in {
    val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
    val taxiFacts = readParquet(taxiFactsFile).rdd

    val dataToWrite = processTaxiData(taxiFacts)
    val actualData = dataToWrite.collect()
    assert(!actualData.isEmpty)
    assert(actualData.head == "11 - 12 : 22121")
    assert(actualData.last == "21 - 22 : 1586")
  }
}
