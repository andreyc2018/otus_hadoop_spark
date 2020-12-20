package edu.lesson_3.collectfiles

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}

import java.net.URI

class FSOps(root: String) {
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI(root), conf)

  def listDirs(path: String, filter: String): Array[FileStatus] = {
    val dirFilter = new GlobFilter(filter)
    val srcPath = new Path(path)
    fs.listStatus(srcPath, dirFilter).filter(file => file.isDirectory)
  }

  def makeDirs(path: String) = {
    fs.mkdirs()
  }

  def listFiles(dir: String, filter: String) = {}
  def appendToFile(src: String, dst: String) = {}
}
