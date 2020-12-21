package edu.lesson_3.collectfiles

import org.apache.hadoop.fs._

import scala.collection.mutable.ArrayBuffer

object FilesAggregator extends App {

  private val fs_ops: FSOps = new FSOps("hdfs://localhost:9000")
  private val srcDirs: Array[FileStatus] = fs_ops.listDirs("/stage", "date=*")

  def collectData(filter: String)(file: FileStatus): FileData = {
    val srcFiles = fs_ops.listFiles(file.getPath, filter)
    val data = srcFiles.foldLeft(ArrayBuffer[Byte]()) { (data, f) => data ++= fs_ops.readFile(f.getPath) }
    return FileData(file.getPath.getName, data.toArray)
  }

  def writeData(file: String, data: Array[Byte]) = {
    fs_ops.writeToFile(file, data)
  }

  srcDirs.map(f => {
    val fileData = collectData("*.csv")(f)
    if (fileData.data.length > 0) {
      writeData("/ods/" + fileData.dir + "/part-0000.csv", fileData.data)
    }
  })
}
