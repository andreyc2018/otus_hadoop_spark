package homework

import homework.DataApiHomeWorkTaxi.{init, readInfo, readStats, taxiFactsFile, taxiInfoFile}
import org.apache.spark.sql.functions.col

object MostPopularRDD {

  def run(): Unit = {

    println("DataFrame: Какие районы самые популярные для заказов?")

    val spark = init()

    val taxiFactsDF = readStats(taxiFactsFile, spark)
    val taxiInfoDF  = readInfo(taxiInfoFile, spark)

    val mostPopularDF = taxiFactsDF
      .join(taxiInfoDF, col("PULocationID") <=> col("LocationID"))
      .groupBy(col("Borough"))
      .count
      .orderBy(col("count").desc)

    mostPopularDF
      .repartition(4)
      .write
      .mode("overwrite")
      .parquet("src/main/resources/data/most_popular_pu_location")

    val readBackDF = readStats("src/main/resources/data/most_popular_pu_location", spark)
    readBackDF.show()

    spark.close()
  }

}
