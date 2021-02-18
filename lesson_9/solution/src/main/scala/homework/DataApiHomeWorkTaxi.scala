package homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, hour, sort_array}

import java.io.File
import java.io.PrintWriter

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

  def writeHoursToFile(filename: String, lines: Seq[(Int, Int)]): Unit = {
    val writer = new PrintWriter(new File(filename))
    writer.write("Часы : Количество вызовов\n")
    lines.foreach(line => writer.write(s"${line._1} - ${line._1 + 1} : ${line._2}\n"))
    writer.close()
  }

  def timeOfMostRequests(): Unit = {
    val spark = init()

    val taxiFactsDF = readStats("src/main/resources/data/yellow_taxi_jan_25_2018", spark)

    val taxiFactsRDD = taxiFactsDF.rdd

    val timesRDD = taxiFactsRDD
      .map(f => f(1).asInstanceOf[java.sql.Timestamp].getHours)
      .map(f => (f, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(p => p._2, false)

    val dataToWrite = timesRDD.collect()
    writeHoursToFile("src/main/resources/data/times_of_most_requests.txt", dataToWrite)
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
}

