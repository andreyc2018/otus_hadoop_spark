package lesson

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

object OrderDistribution extends App {

  val sparkSession = SparkSession.builder()
    .appName("Order Distribution")
    .config("spark.master", "local")
    .getOrCreate()

//     case class TaxiFacts()
  case class TaxiFacts(VendorID: Int,
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
                        total_amount: Double)

  val taxiFactsDF = sparkSession.read.load("src/main/resources/data/small_set")
  import sparkSession.implicits._

  val taxiFactsDS = taxiFactsDF.as[TaxiFacts]
//  taxiFactsDS.show(1)
//  taxiFactsDS.printSchema()
  taxiFactsDS.limit(2).foreach(f => println(f.tpep_pickup_datetime.getHours))

  sparkSession.close()
}
