package homework

import homework.DataApiHomeWorkTaxi.{init, readStats, taxiFactsFile}
import org.apache.hadoop.fs.Path

import java.sql.Timestamp

object TimeOfMostRequestsDF {

  def run(): Unit = {

    println("RDD: В какое время происходит больше всего вызовов?")
    val spark = init()

    val taxiFactsDF = readStats(taxiFactsFile, spark)

    val taxiFactsRDD = taxiFactsDF.rdd

    val timesRDD = taxiFactsRDD
      .map(f => f(1).asInstanceOf[Timestamp].getHours)
      .map(f => (f, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(p => p._2, false)

    val dataToWrite = timesRDD.map(p => s"${p._1} - ${p._1 + 1} : ${p._2}")

    val destDir = "src/main/resources/data/times_of_most_requests"

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(destDir)))
      fs.delete(new Path(destDir), true)

    dataToWrite.repartition(1).saveAsTextFile(destDir)
    println(dataToWrite.first())

    spark.close()
  }

}
