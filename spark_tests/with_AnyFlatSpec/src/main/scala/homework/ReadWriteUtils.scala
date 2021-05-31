package homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path

object ReadWriteUtils {

  def readParquet(path: String)(implicit spark: SparkSession) : DataFrame =
    spark.read.load(path)

  def writeRDDAsText(rdd: RDD[String], path: String)(implicit spark: SparkSession) : Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(path))) {
      fs.delete(new Path(path), true)
    }
    rdd.repartition(1).saveAsTextFile(path)
  }
}
