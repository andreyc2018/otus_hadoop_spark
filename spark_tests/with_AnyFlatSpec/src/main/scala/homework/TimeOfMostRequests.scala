package homework

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD

import java.sql.Timestamp

object TimeOfMostRequests extends App {
  import ReadWriteUtils._

  println("RDD: В какое время происходит больше всего вызовов?")

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Homework #8")
    .config("spark.master", "local")
    .getOrCreate()

  def processTaxiData(taxiRDD: RDD[Row]) : RDD[String] =
    taxiRDD
      .map(f => f(1).asInstanceOf[Timestamp].getHours)
      .map(f => (f, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(p => p._2, false)
      .map(p => s"${p._1} - ${p._1 + 1} : ${p._2}")

  val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiFactsRDD = readParquet(taxiFactsFile).rdd
  val resultRDD = processTaxiData(taxiFactsRDD)
  val destDir = "src/main/resources/result/times_of_most_requests"
  writeRDDAsText(resultRDD, destDir)
  println(resultRDD.first())

  spark.close()
}
