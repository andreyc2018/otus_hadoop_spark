package lesson

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OtusRDD extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val context: SparkContext = spark.sparkContext

  case class TaxiZone(locationId: Int,
                      borough: String,
                      zone: String,
                      serviceZone: String)

  val rdd_1 = context.textFile("src/main/resources/data/taxi_zones_1.csv")
  rdd_1.foreach(x => println(x))

  val rdd_2 = rdd_1.map(l => l.split(","))
  rdd_2.foreach(x => println(s"${x(0)} : ${x(1)} : ${x(2)}"))

  val rdd_3 = rdd_2.filter(t => t(3).toUpperCase() == t(3))
  val rdd_4 = rdd_3.map(t => TaxiZone(t(0).toInt, t(1), t(2), t(3)))
  val rdd_5 = rdd_4.map(tz => (tz.borough, 1))
  val rdd_6 = rdd_5.reduceByKey(_ + _)

  rdd_6.foreach(x => println(s"${x._1} -> ${x._2}"))
}

