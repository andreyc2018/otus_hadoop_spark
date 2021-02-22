package homework

import homework.DataApiHomeWorkTaxi.init
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object OrderDistributionDS {

  def run(): Unit = {

    println("DataSet: Как происходит распределение заказов?")
    val spark = init()

    val taxiFactsDF = spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")

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

}
