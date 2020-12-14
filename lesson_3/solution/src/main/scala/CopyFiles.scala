import org.slf4j.LoggerFactory
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI
import java.io.File

object CopyFiles extends App {

  def appendToFile(srdDir: String, dstFile: String): Unit = {

  }

  def showFile(fs: FileSystem, dstPath: String)(file: FileStatus): Unit = {
    println("dst = " + dstPath.toString)
    println("copy file = " + file.getPath.getName)
    println("copy file = " + file.toString)
    val srcFile = new Path("/" + file.getPath.getParent.getName + "/" + file.getPath.getName)
    println("srcFile = " + srcFile.toString)
    val readFromFile: FSDataInputStream = fs.open(file.getPath)
    println("Read from = " + readFromFile.toString)

    readFromFile.readFully(buffer)
  }

  def processDir(fs: FileSystem, dstPath: String)(file: FileStatus): Unit = {
    println("process dir = " + file.getPath.getName)

    val fileFilter = new GlobFilter("*.csv")
//    val dst = new Path("/" + file.getPath.getParent.getName + "/" + file.getPath.getName)
    val srcFiles = fs.listStatus(file.getPath, fileFilter).filter(file => (file.isFile == true))
    srcFiles.map(showFile(fs, dstPath))
  }

  val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  logger.info("Starting")
  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val dirFilter = new GlobFilter("date=*")
  val srcPath = new Path("/stage")
  val srcDirs = fileSystem.listStatus(srcPath, dirFilter).filter(file => (file.isDirectory == true))
  srcDirs.map(processDir(fileSystem, "/ods"))
//  val lfs = it.next()
//  val name = lfs.getPath().getName()
//  logger.info("lfs = {}, name = {}, path = {}", lfs, name, lfs.getPath())
//  println(it)
//  val file: FSDataInputStream = fileSystem.open(path)
//  println(file)
}
