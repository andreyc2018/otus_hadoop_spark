package edu.lesson_3.collectfiles

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.slf4j.LoggerFactory

import java.net.URI

object FilesAggregator extends App {

  def appendToFile(srdDir: String, dstFile: String): Unit = {

  }

  def showFile(fs: FileSystem, dstFile: String)(file: FileStatus): Unit = {
    println("dst file = " + dstFile.toString)
    println("src file = " + file.getPath)
    println("file = " + file.toString)
    val srcFile = new Path("/" + file.getPath.getParent.getName + "/" + file.getPath.getName)
    println("srcFile = " + srcFile.toString)
    val readFromFile: FSDataInputStream = fs.open(file.getPath)
    println("Read from = " + readFromFile.toString)

    //    readFromFile.readFully(buffer)
  }

  def processDir(fs: FileSystem, dstRoot: String, filter: String)(file: FileStatus): Unit = {
//    println("process dir = " + file.getPath.getName)
//    println("process path = " + file.getPath)
//    println("file status = " + file.getPath)
    val dstFile = dstRoot + "/" + file.getPath.getName + "/" + "part-0000.csv"
//    val fileFilter = new GlobFilter("*.csv")
//    val dst = new Path("/" + file.getPath.getParent.getName + "/" + file.getPath.getName)
//    val srcFiles = fs.listStatus(file.getPath, fileFilter).filter(file => file.isFile)
    val srcFiles = listFiles(fs, file.getPath, filter)
    srcFiles.foreach(showFile(fs, dstFile))
  }

  def listFiles(fs: FileSystem, path: Path, filter: String): Array[FileStatus] = {
    val fileFilter = new GlobFilter(filter)
    fs.listStatus(path, fileFilter).filter(file => file.isFile)
  }

  def listDirs(fs: FileSystem, path: String, filter: String): Array[FileStatus] = {
    val dirFilter = new GlobFilter(filter)
    val srcPath = new Path(path)
    fs.listStatus(srcPath, dirFilter).filter(file => file.isDirectory)
  }

  val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  logger.info("Starting")
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val srcDirs = listDirs(fs, "/stage", "date=*")
  srcDirs.foreach(processDir(fs, "/ods", "*.csv"))
  //  val lfs = it.next()
  //  val name = lfs.getPath().getName()
  //  logger.info("lfs = {}, name = {}, path = {}", lfs, name, lfs.getPath())
  //  println(it)
  //  val file: FSDataInputStream = fileSystem.open(path)
  //  println(file)
}
