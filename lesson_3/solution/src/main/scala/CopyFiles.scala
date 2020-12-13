import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object CopyFiles extends App {
  val logger = Logger("name")
  logger.info("Starting")
  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val filename = "/stage/date=2020-12-03/part-0000.csv"
  val path = new Path(filename)
  val file: FSDataInputStream = fileSystem.open(path)
  println(file)
}
