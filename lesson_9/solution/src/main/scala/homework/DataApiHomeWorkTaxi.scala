package homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sort_array}

object DataApiHomeWorkTaxi extends App {

  def readStats(path: String, spark: SparkSession): DataFrame = {
    spark.read.load(path)
  }

  def readInfo(path: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def init(): SparkSession = {
    SparkSession.builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()
  }

  def mostPopular(): Unit = {
    val spark = init()

    val taxiFactsDF = readStats("src/main/resources/data/yellow_taxi_jan_25_2018", spark)
    val taxiInfoDF = readInfo("src/main/resources/data/taxi_zones.csv", spark)

    val mostPopularDF = taxiFactsDF
      .join(taxiInfoDF, col("PULocationID") <=> col("LocationID"))
      .groupBy(col("Borough"))
      .agg(count("*").as("count"))
      .orderBy(col("count").desc)

    mostPopularDF
      .repartition(4)
      .write
      .mode("overwrite")
      .parquet("src/main/resources/data/most_popular_pu_location")

    val readBackDF = readStats("src/main/resources/data/most_popular_pu_location", spark)
    readBackDF.show()

    spark.close()
  }

  def timeOfMostRequests(): Unit = {
    val spark = init()

    val taxiFactsDF = readStats("src/main/resources/data/yellow_taxi_jan_25_2018", spark)
    val taxiInfoDF = readInfo("src/main/resources/data/taxi_zones.csv", spark)

    val taxiFactsRDD = taxiFactsDF.rdd

    taxiFactsRDD.foreach(f => println(f))
    spark.close()
  }

//  case class TaxiZone(LocationID:   String,
//                      Borough:      String,
//                      Zone:         String,
//                      service_zone:  String)
//
//  val taxiZoneDF = spark.read
//    .option("header", "true")
//    .csv("src/main/resources/data/taxi_zones.csv")

//  val driver = "org.postgresql.Driver"
//  val url = "jdbc:postgresql://localhost:5432/otus"
//  val user = "docker"
//  val password = "docker"

  /**
   * Задание написать код, который будет делать следующее:
   *
   * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
   * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
   * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
   */

  // * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
//  mostPopular()

  // * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
  timeOfMostRequests()
}

