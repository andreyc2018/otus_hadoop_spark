package homework

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataApiHomeWorkTaxi extends App {

  val taxiFactsFile      = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiInfoFile       = "src/main/resources/data/taxi_zones.csv"
  val smallTaxiFactsFile = "src/main/resources/data/small_set"

  def init(): SparkSession = {
    SparkSession
      .builder()
      .appName("Homework #12")
      .config("spark.master", "local")
      .getOrCreate()
  }

  def readStats(path: String, spark: SparkSession): DataFrame = {
    spark.read.load(path)
  }

  def readInfo(path: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  /*
  # Домашнее задание
  ## Основная инструкция задание 2:
    Сформировать ожидаемый результат и покрыть простым тестом (с библиотекой AnyFlatSpec)
    витрину из домашнего задания к занятию Spark Data API, построенную с помощью RDD.
    Пример src/test/scala/lesson2/SimpleUnitTest.scala
  */

  MostPopularDF.run()

  TimeOfMostRequestsRDD.run()

  OrderDistributionDS.run()
}
