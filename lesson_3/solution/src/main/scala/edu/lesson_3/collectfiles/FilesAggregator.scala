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

  def processDir(fs: FileSystem, dstRoot: String)(file: FileStatus): Unit = {
    println("process dir = " + file.getPath.getName)
    println("process path = " + file.getPath)
    val dstFile = dstRoot + "/" + file.getPath.getName + "/" + "part-0000.csv"
    val fileFilter = new GlobFilter("*.csv")
    //    val dst = new Path("/" + file.getPath.getParent.getName + "/" + file.getPath.getName)
    val srcFiles = fs.listStatus(file.getPath, fileFilter).filter(file => file.isFile)
    srcFiles.foreach(showFile(fs, dstFile))
  }

  val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  logger.info("Starting")
  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val dirFilter = new GlobFilter("date=*")
  val srcPath = new Path("/stage")
  val srcDirs = fileSystem.listStatus(srcPath, dirFilter).filter(file => file.isDirectory)
  srcDirs.foreach(processDir(fileSystem, "/ods"))
  //  val lfs = it.next()
  //  val name = lfs.getPath().getName()
  //  logger.info("lfs = {}, name = {}, path = {}", lfs, name, lfs.getPath())
  //  println(it)
  //  val file: FSDataInputStream = fileSystem.open(path)
  //  println(file)
}
