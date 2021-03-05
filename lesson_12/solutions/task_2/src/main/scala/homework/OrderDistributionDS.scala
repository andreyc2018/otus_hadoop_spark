package homework

import homework.DataApiHomeWorkTaxi.init
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._

import java.sql.Timestamp

object OrderDistributionDS {

  def run(): Unit = {

    println("DataSet: Как происходит распределение заказов?")
    val spark = init()

    val taxiFactsDF = spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")

    val taxiStats = process(spark, taxiFactsDF)

    val opts = Map(
      "url"      -> "jdbc:postgresql://localhost:5432/otus",
      "dbtable"  -> "trip_stats",
      "user"     -> "docker",
      "password" -> "docker"
    )
    save(taxiStats, opts)

    val showDF = spark.read.format("jdbc").options(opts).load()
    showDF.orderBy("hour").show(24)

    spark.close()
  }

  def process(spark: SparkSession, facts: DataFrame): DataFrame = {
    val tsHour: (Timestamp) => Int = (ts: Timestamp) => { ts.getHours }
    val tsHourUDF = udf(tsHour)
    val withHour = facts
      .withColumn("Hour", tsHourUDF(facts.col("tpep_pickup_datetime")))

    import spark.implicits._

    val taxiFactsDS = withHour.as[TaxiFacts]

    taxiFactsDS
      .groupBy("Hour")
      .agg(
        count("trip_distance").name("trips"),
        avg("trip_distance").name("avg_dist"),
        max("trip_distance").name("max_dist"),
        min("trip_distance").name("min_dist"),
        stddev("trip_distance").name("stddev_dist")
      )
  }

  def save(stats: DataFrame, opts: Map[String, String]): Unit = {
    stats.write.mode("overwrite").format("jdbc").options(opts).save()
  }
}
