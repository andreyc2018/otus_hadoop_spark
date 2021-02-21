package homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

import java.sql.Timestamp

object DataApiHomeWorkTaxi extends App {

  val taxiFactsFile      = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiInfoFile       = "src/main/resources/data/taxi_zones.csv"
  val smallTaxiFactsFile = "src/main/resources/data/small_set"

  def init(): SparkSession = {
    SparkSession
      .builder()
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

    val taxiFactsDF = readStats(taxiFactsFile, spark)
    val taxiInfoDF  = readInfo(taxiInfoFile, spark)

    val mostPopularDF = taxiFactsDF
      .join(taxiInfoDF, col("PULocationID") <=> col("LocationID"))
      .groupBy(col("Borough"))
      .count
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

    val taxiFactsDF = readStats(taxiFactsFile, spark)

    val taxiFactsRDD = taxiFactsDF.rdd

    val timesRDD = taxiFactsRDD
      .map(f => f(1).asInstanceOf[Timestamp].getHours)
      .map(f => (f, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(p => p._2, false)

    val dataToWrite = timesRDD.map(p => s"${p._1} - ${p._1 + 1} : ${p._2}")

    val destDir = "src/main/resources/data/times_of_most_requests"

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(destDir)))
      fs.delete(new Path(destDir), true)

    dataToWrite.repartition(1).saveAsTextFile(destDir)
    println(dataToWrite.first())

    spark.close()
  }

  // case class TaxiFacts(
  //     VendorID: Int,
  //     tpep_pickup_datetime: Timestamp,
  //     tpep_dropoff_datetime: Timestamp,
  //     passenger_count: Int,
  //     trip_distance: Double,
  //     RatecodeID: Int,
  //     store_and_fwd_flag: String,
  //     PULocationID: Int,
  //     DOLocationID: Int,
  //     payment_type: Int,
  //     fare_amount: Double,
  //     extra: Double,
  //     mta_tax: Double,
  //     tip_amount: Double,
  //     tolls_amount: Double,
  //     improvement_surcharge: Double,
  //     total_amount: Double,
  //     Hour: Int
  // )

  def orderDistribution(): Unit = {

    println("DataSet: Как происходит распределение заказов?")
    val spark = init()

    val tsHour: (Timestamp) => Int = (ts: Timestamp) => { ts.getHours }

    val tsHourUDF = udf(tsHour)

    val taxiFactsDF = spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")
    val withHour = taxiFactsDF
      .withColumn("Hour", tsHourUDF(taxiFactsDF.col("tpep_pickup_datetime")))

    import spark.implicits._

    val taxiFactsDS = withHour.as[TaxiFacts]

    val tripsStats = taxiFactsDS
      .groupBy("Hour")
      .agg(
        count("trip_distance").name("trips"),
        avg("trip_distance").name("avg_dist"),
        max("trip_distance").name("max_dist"),
        min("trip_distance").name("min_dist"),
        stddev("trip_distance").name("stddev_dist")
      )

    val opts = Map(
      "url"      -> "jdbc:postgresql://localhost:5432/otus",
      "dbtable"  -> "trip_stats",
      "user"     -> "docker",
      "password" -> "docker"
    )
    tripsStats.write.mode("overwrite").format("jdbc").options(opts).save()

    val showDF = spark.read.format("jdbc").options(opts).load()
    showDF.orderBy("hour").show(24)

    spark.close()
  }

  /** Задание написать код, который будет делать следующее:
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
