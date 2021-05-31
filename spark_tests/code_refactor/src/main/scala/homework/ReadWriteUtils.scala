package homework

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path

object ReadWriteUtils {

  def readParquet(path: String)(implicit spark: SparkSession) : DataFrame =
    spark.read.load(path)

  def readCSV(path: String)(implicit spark: SparkSession) : DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def readPG(opts: Map[String, String])(implicit spark: SparkSession): DataFrame =
    spark.read.format("jdbc").options(opts).load()

  def writeParquet(resultDF: DataFrame, path: String) : Unit =
    resultDF
      .repartition(4)
      .write
      .mode("overwrite")
      .parquet(path)

  def writeRDDAsText(rdd: RDD[String], path: String)(implicit spark: SparkSession) : Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(path))) {
      fs.delete(new Path(path), true)
    }
    rdd.repartition(1).saveAsTextFile(path)
  }

  def writePG(data: Dataset[Row], opts: Map[String, String]): Unit =
    data.write.mode("overwrite").format("jdbc").options(opts).save()
}
