package homework

import homework.DataApiHomeWorkTaxi.{init, readInfo, readStats, taxiFactsFile, taxiInfoFile}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object MostPopularDF {

  def run(): Unit = {

    println("DataFrame: Какие районы самые популярные для заказов?")

    val spark = init()

    val taxiFactsDF = readStats(taxiFactsFile, spark)
    val taxiInfoDF  = readInfo(taxiInfoFile, spark)

    val mostPopularDF = process(taxiFactsDF, taxiInfoDF)

    val destDir = "src/main/resources/data/most_popular_pu_location"
    save(destDir, mostPopularDF)

    val readBackDF = readStats(destDir, spark)
    readBackDF.show()

    spark.close()
  }

  def process(facts: DataFrame, info: DataFrame): DataFrame = {
    facts
      .join(info, col("PULocationID") <=> col("LocationID"))
      .groupBy(col("Borough"))
      .count
      .orderBy(col("count").desc)
  }

  def save(dest: String, data: DataFrame): Unit = {
    data
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(dest)
  }
}
