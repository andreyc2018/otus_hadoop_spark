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

  /** Задание написать код, который будет делать следующее:
    *
    * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
    * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
    * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
    */

  // * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
  MostPopularRDD.run()

  // * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
  TimeOfMostRequestsDF.run()

  // * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
  OrderDistributionDS.run()
}
