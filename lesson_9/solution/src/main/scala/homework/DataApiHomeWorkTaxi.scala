package homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, hour, sort_array}

import java.io.File
import java.io.PrintWriter
import scala.reflect.io.Directory

object DataApiHomeWorkTaxi extends App {

  def init(): SparkSession = {
    SparkSession.builder()
      .appName("Homework #9")
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

  def mostPopular(): Unit = {

    println("DataFrame: Какие районы самые популярные для заказов?")

    val spark = init()

    val taxiFactsDF = readStats("src/main/resources/data/yellow_taxi_jan_25_2018", spark)
    val taxiInfoDF = readInfo("src/main/resources/data/taxi_zones.csv", spark)

    val mostPopularDF = taxiFactsDF
      .join(taxiInfoDF, col("PULocationID") <=> col("LocationID"))
      .groupBy(col("Borough")).count
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

    println("RDD: В какое время происходит больше всего вызовов?")
    val spark = init()

    val taxiFactsDF = readStats("src/main/resources/data/yellow_taxi_jan_25_2018", spark)

    val taxiFactsRDD = taxiFactsDF.rdd

    val timesRDD = taxiFactsRDD
      .map(f => f(1).asInstanceOf[java.sql.Timestamp].getHours)
      .map(f => (f, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(p => p._2, false)

    val dataToWrite = timesRDD.map(p => (p._1.toString + " - " + (p._1 + 1).toString + " : " + p._2.toString))

    val destDir = "src/main/resources/data/times_of_most_requests"
    val dir = new Directory(new File(destDir))
    dir.deleteRecursively()
    dataToWrite.repartition(1).saveAsTextFile(destDir)

    spark.close()
  }

  def orderDistribution(): Unit = {

    println("DataSet: Как происходит распределение заказов?")
    val spark = init()

    spark.close()
  }
  /**
   * Задание написать код, который будет делать следующее:
   *
   * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
   * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
   * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
   */

  // * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
  mostPopular()

  // * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
  timeOfMostRequests()

  // * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
  orderDistribution()
}

