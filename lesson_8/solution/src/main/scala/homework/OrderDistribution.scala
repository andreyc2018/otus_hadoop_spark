package homework

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, FileInputStream}
import java.sql.Timestamp
import java.util.Properties

object OrderDistribution extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("Order Distribution")
    .config("spark.master", "local")
    .getOrCreate()

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
      total_amount: Double,
      Hour: Int
  )

  val taxiFactsDF = sparkSession.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  val tsHour: (Timestamp) => Int = (ts: Timestamp) => { ts.getHours }

  val tsHourUDF = udf(tsHour)
  val output = taxiFactsDF.withColumn("Hour",tsHourUDF(taxiFactsDF.col("tpep_pickup_datetime")))
  // output.show(5)
  import sparkSession.implicits._

  val taxiFactsDS = output.as[TaxiFacts]
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

  val tripsStats = taxiFactsDS.groupBy("Hour").agg(
    count("trip_distance").name("trips"),
    avg("trip_distance").name("avg_dist"),
    max("trip_distance").name("max_dist"),
    min("trip_distance").name("min_dist"),
    stddev("trip_distance").name("stddev_dist")
  )
  // tripsStats.show()

  // The following if/else does the same using conf file approach and inline Map approach
  // The conf file is more usefull because all parameters are defined outside of the source code.
  val useDbConf = false
  if (useDbConf) {
    val dbProperties = new Properties
    val confFile     = "src/main/resources/db-properties.conf"
    dbProperties.load(new FileInputStream(new File(confFile)));
    val jdbcUrl = dbProperties.getProperty("jdbcUrl")
    val where   = "trip_stats"
    tripsStats.write.mode("overwrite").jdbc(jdbcUrl, where, dbProperties)
    val showDF = sparkSession.read.jdbc(jdbcUrl, where, dbProperties)
    showDF.show()
  } else {
    val opts = Map(
      "url"      -> "jdbc:postgresql://localhost:5432/otus",
      "dbtable"  -> "trip_stats",
      "user"     -> "docker",
      "password" -> "docker"
    )
    tripsStats.write.mode("overwrite").format("jdbc").options(opts).save()

    val showDF = sparkSession.read.format("jdbc").options(opts).load()
    showDF.orderBy("hour").show(23)
  }

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