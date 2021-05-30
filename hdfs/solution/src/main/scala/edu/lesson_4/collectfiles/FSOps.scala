package edu.lesson_4.collectfiles

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}

import java.io.BufferedInputStream
import java.net.URI
import scala.collection.mutable.ArrayBuffer

class FSOps(root: String) {
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI(root), conf)

  def deleteEntry(path: String): Boolean = {
    fs.delete(new Path(path), true)
  }

  def listDirs(path: String, filter: String): Array[FileStatus] = {
    fs.listStatus(new Path(path), new GlobFilter(filter)).filter(file => file.isDirectory)
  }

  def listFiles(path: Path, filter: String): Array[FileStatus] = {
    fs.listStatus(path, new GlobFilter(filter)).filter(file => file.isFile)
  }

  def listFiles(path: String, filter: String): Array[FileStatus] = {
    listFiles(new Path(path), filter)
  }

  def fileSize(path: String): Long = {
    val status: FileStatus = fs.getFileStatus(new Path(path))
    if (status.isFile) {
      return status.getLen
    }
    return 0
  }

  def makeDirs(path: String): Boolean = {
    val dir = new Path(path)
    if (!fs.exists(dir)) {
      return fs.mkdirs(dir)
    }
    return true
  }

  def readFile(path: String): ArrayBuffer[Byte] = {
    readFile(new Path(path))
  }

  def readFile(path: Path): ArrayBuffer[Byte] = {
    val in = new BufferedInputStream(fs.open(path))
    val byteArray = new Array[Byte](1024)
    var data = ArrayBuffer[Byte]()
    var numBytes = 0
    do {
      numBytes = in.read(byteArray)
      if (numBytes > 0) {
        data ++= byteArray.slice(0, numBytes)
      }
    } while (numBytes > 0)

    if (data.nonEmpty && data.last != '\n') {
      data += '\n'
    }

    return data
  }

  def writeToFile(path: String, data: Array[Byte]): Unit = {
    val out = fs.create(new Path(path))
    out.write(data)
    out.close()
  }
}
