package homework

import homework.DataApiHomeWorkTaxi.readStats
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.awt.image.DataBuffer

object CreateSubset extends App {
  def init(): SparkSession = {
    SparkSession
      .builder()
      .appName("Homework #9")
      .config("spark.master", "local")
      .getOrCreate()
  }

  val spark = init()

  val taxiFactsDF: DataFrame =
    readStats("src/main/resources/data/yellow_taxi_jan_25_2018", spark)

  taxiFactsDF.printSchema()

  val mostPopularDF = taxiFactsDF
    .limit(10)

  mostPopularDF
    .write
    .mode("overwrite")
    .parquet("src/main/resources/data/small_set")

  val readBackDF = readStats("src/main/resources/data/small_set", spark)
  readBackDF.show()
}
