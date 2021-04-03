package homework

import org.apache.spark.sql.{DataFrame, SparkSession}

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
    spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  taxiFactsDF.printSchema()

  val smallDF = taxiFactsDF.limit(10)

  smallDF
    .write
    .mode("overwrite")
    .parquet("src/main/resources/data/small_set")

  val readBackDF = spark.read.load("src/main/resources/data/small_set")
  readBackDF.show()

  smallDF
    .limit(1)
    .coalesce(1)
    .write.format("csv")
    .option("header", "true")
    .save("src/main/resources/data/taxi_info_small")
}
