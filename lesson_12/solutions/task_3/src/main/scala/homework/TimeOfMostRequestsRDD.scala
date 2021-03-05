package homework

import homework.DataApiHomeWorkTaxi.{init, readStats, taxiFactsFile}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

import java.sql.Timestamp

object TimeOfMostRequestsRDD {

  def run(): Unit = {

    println("RDD: В какое время происходит больше всего вызовов?")
    val destDir = "src/main/resources/data/times_of_most_requests"

    implicit val spark = init()

    val taxiFactsDF = readStats(taxiFactsFile)

    val dataToWrite = process(taxiFactsDF.rdd)
    save(spark, destDir, dataToWrite)

    println(dataToWrite.first())

    spark.close()
  }

  def process(facts: RDD[Row]): RDD[String] = {
    val timesRDD = facts
      .map(f => f(1).asInstanceOf[Timestamp].getHours)
      .map(f => (f, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(p => p._2, false)

    timesRDD.map(p => s"${p._1} - ${p._1 + 1} : ${p._2}")
  }

  def save(spark: SparkSession, destDir: String, data: RDD[String]): Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(destDir)))
      fs.delete(new Path(destDir), true)

    data.repartition(1).saveAsTextFile(destDir)
  }
}
