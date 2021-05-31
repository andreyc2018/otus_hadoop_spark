package homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MostPopular extends App {
  import ReadWriteUtils._

  println("DataFrame: Какие районы самые популярные для заказов?")

  def processTaxiData(taxiDF: DataFrame, taxiZonesDF: DataFrame)(implicit spark: SparkSession) : DataFrame =
    taxiDF
      .join(taxiZonesDF, col("PULocationID") <=> col("LocationID"))
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total_trips"),
        max("trip_distance").as("max_distance"))
      .orderBy(col("total_trips").desc)

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Homework #8")
    .config("spark.master", "local")
    .getOrCreate()
  val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiInfoFile = "src/main/resources/data/taxi_zones.csv"
  val taxiFactsDF = readParquet(taxiFactsFile)
  val taxiInfoDF = readCSV(taxiInfoFile)
  val mostPopularDF = processTaxiData(taxiFactsDF, taxiInfoDF)

  val resultFile = "src/main/resources/result/most_popular_pu_location"
  writeParquet(mostPopularDF, resultFile)

  val readBackDF = readParquet(resultFile)
  readBackDF.show()

  spark.close()
}
