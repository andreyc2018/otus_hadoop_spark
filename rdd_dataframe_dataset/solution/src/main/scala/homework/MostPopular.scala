package homework

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MostPopular extends App {
  println("DataFrame: Какие районы самые популярные для заказов?")

  val spark = SparkSession
    .builder()
    .appName("Homework #8")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiInfoFile = "src/main/resources/data/taxi_zones.csv"

  val taxiFactsDF = spark.read.load(taxiFactsFile)
  val taxiInfoDF  = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(taxiInfoFile)

  val mostPopularDF = taxiFactsDF
    .join(taxiInfoDF, col("PULocationID") <=> col("LocationID"))
    .groupBy(col("Borough"))
    .count
    .orderBy(col("count").desc)

  val resultFile = "src/main/resources/result/most_popular_pu_location"
  mostPopularDF
    .repartition(4)
    .write
    .mode("overwrite")
    .parquet(resultFile)

  val readBackDF = spark.read.load(resultFile)
  readBackDF.show()

  spark.close()

}
