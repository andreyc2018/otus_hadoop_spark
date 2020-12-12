import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI

object CopyFiles extends App {
  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val filename = "hello.csv"
  val path = new Path(filename)
  val file: FSDataInputStream = fileSystem.open(path)
  println(file)
}
