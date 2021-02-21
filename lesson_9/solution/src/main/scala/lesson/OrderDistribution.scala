package lesson

import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, max, min, stddev}

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.util.Properties

object OrderDistribution extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("Order Distribution")
    .config("spark.master", "local")
    .getOrCreate()

//     case class TaxiFacts()
  case class TaxiFacts(
      VendorID: Int,
      tpep_pickup_datetime: Timestamp,
      tpep_dropoff_datetime: Timestamp,
      passenger_count: Int,
      trip_distance: Double,
      RatecodeID: Int,
      store_and_fwd_flag: String,
      PULocationID: Int,
      DOLocationID: Int,
      payment_type: Int,
      fare_amount: Double,
      extra: Double,
      mta_tax: Double,
      tip_amount: Double,
      tolls_amount: Double,
      improvement_surcharge: Double,
      total_amount: Double
  )

  val taxiFactsDF = sparkSession.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  import sparkSession.implicits._

  val taxiFactsDS = taxiFactsDF.as[TaxiFacts]
//  val taxiFactsPerHourDS = taxiFactsDS.withColumn("Hour", s"${_.tpep_pickup_datetime.getHours}")
//  taxiFactsDS.show(1)
//  taxiFactsDS.printSchema()
//  taxiFactsDS.limit(2).foreach(f => println(f.tpep_pickup_datetime.getHours))
//  val perHour: KeyValueGroupedDataset[Int, TaxiFacts] = taxiFactsDS.groupByKey(d => d.tpep_pickup_datetime.getHours)
//  val trips: Dataset[(Int, Long)] = perHour.count()
//  val avgDist = perHour.flatMapGroups()
//    .agg(avg("trip_distance").name("avg"))
//  val a = totalTrips.mapGroups(f => )

//  totalTrips.printSchema()
//  totalTrips.show()

  val tripsStats: DataFrame = taxiFactsDS.agg(
    count("trip_distance").name("trips"),
    avg("trip_distance").name("avg_dist"),
    max("trip_distance").name("max_dist"),
    min("trip_distance").name("min_dist"),
    stddev("trip_distance").name("stddev_dist")
  )
//  taxiStatsDS.printSchema()
  tripsStats.show()

  val opts = Map(
    "url"      -> "jdbc:postgresql://localhost:5432/otus",
    "dbtable"  -> "trip_stats",
    "user"     -> "docker",
    "password" -> "docker"
  )

//  val dbProperties = new Properties
//  val confFile = "src/main/resources/db-properties.flat"
//  dbProperties.load(new FileInputStream(new File(confFile)));
//  val jdbcUrl = dbProperties.getProperty("jdbcUrl")
//  val where = "trip_stats"
//  tripsStats.write.mode("append").jdbc(jdbcUrl, where, dbProperties)

  tripsStats.write.mode("append").format("jdbc").options(opts).save()

//  val showDF = sparkSession.read.jdbc(jdbcUrl, where, dbProperties)
//  showDF.show()

  //  val perHourDS = taxiFactsDS
//    .groupByKey(d => d.tpep_pickup_datetime.getHours)
////    .map
////    .agg(count("tpep_pickup_datetime").name("count_trips"), avg("trip_distance").name("avg_dist"))
//  perHourDS.mapValues(v => {
//
//  })

  //    .agg(
//      count("tpep_pickup_datetime"),
//      avg("trip_distance")
//    )
//  val ds_1 = taxiFactsDS.foreach(d => d.tpep_pickup_datetime.getHours)
//  println(s"$ds_1")
  //  _(Пример: можно построить витрину со следующими колонками:
  //  общее количество поездок,
  //  среднее расстояние,
  //  среднеквадратическое отклонение,
  //  минимальное и максимальное расстояние)_

  sparkSession.close()
}
