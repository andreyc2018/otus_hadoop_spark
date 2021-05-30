package homework

/* db.sql
DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;

\connect otus

CREATE TABLE trip_stats (
    trips       INT,
    avg_dist    REAL,
    max_dist    REAL,
    min_dist    REAL,
    stddev_dist REAL
);
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object OrderDistribution extends App {

  println("DataSet: Как происходит распределение заказов?")
  val spark = SparkSession
    .builder()
    .appName("Homework #8")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiFactsFile = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxiFactsDF = spark.read
    .load(taxiFactsFile)

  val tsHour: (Timestamp) => Int = (ts: Timestamp) => { ts.getHours }
  val tsHourUDF = udf(tsHour)
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
    .orderBy("Hour")

  val opts = Map(
    "url"      -> "jdbc:postgresql://localhost:5432/otus",
    "dbtable"  -> "trip_stats",
    "user"     -> "docker",
    "password" -> "docker"
  )
  tripsStats.write.mode("overwrite").format("jdbc").options(opts).save()

  val showDF = spark.read.format("jdbc").options(opts).load()
  showDF.show(24)

  spark.close()
}
