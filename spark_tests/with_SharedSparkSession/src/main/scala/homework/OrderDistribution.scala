package homework

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

object OrderDistribution extends App {
  import ReadWriteUtils._

  println("DataSet: Как происходит распределение заказов?")

  def processTaxiData(taxiDF: DataFrame)(implicit spark: SparkSession): Dataset[Row] = {
    val tsHour: (Timestamp) => Int = (ts: Timestamp) => { ts.getHours }
    val tsHourUDF = udf(tsHour)
    val withHour = taxiDF
      .withColumn("Hour", tsHourUDF(taxiDF.col("tpep_pickup_datetime")))

    import spark.implicits._

    val taxiDS = withHour.as[TaxiFacts]

    taxiDS
      .groupBy("Hour")
      .agg(
        count("trip_distance").name("trips"),
        avg("trip_distance").name("avg_dist"),
        max("trip_distance").name("max_dist"),
        min("trip_distance").name("min_dist"),
        stddev("trip_distance").name("stddev_dist")
      )
      .orderBy("Hour")
  }

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Homework #8")
    .config("spark.master", "local")
    .getOrCreate()
  val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiFactsDF = readParquet(taxiFactsFile)

  val tripsStats = processTaxiData(taxiFactsDF)

  val opts = Map(
    "url"      -> "jdbc:postgresql://localhost:5432/otus",
    "dbtable"  -> "trip_stats",
    "user"     -> "docker",
    "password" -> "docker"
  )
  writePG(tripsStats, opts)

  val showDF = readPG(opts)
  showDF.show(24)

  spark.close()
}
