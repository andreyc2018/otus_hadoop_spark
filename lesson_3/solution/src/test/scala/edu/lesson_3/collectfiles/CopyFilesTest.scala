package edu.lesson_3.collectfiles

//import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, GlobFilter, Path}
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI

class CopyFilesTest extends AnyFunSuite {
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)
  val local_fs: FileSystem = FileSystem.get(new URI("/"), conf)

  test("list remote root") {
    println("listing the remote root dir")
    val dirFilter = new GlobFilter("*")
    val srcPath = new Path("/")
    val dirs = fs.listStatus(srcPath, dirFilter).filter(file => file.isDirectory)
    assert(!dirs.isEmpty)
  }

  test("list local root") {
    println("listing the local root dir")
    val dirFilter = new GlobFilter("*")
    val srcPath = new Path("/")
    val dirs = local_fs.listStatus(srcPath, dirFilter).filter(file => file.isDirectory)
    assert(!dirs.isEmpty)

    val cwd = local_fs.getWorkingDirectory()
    println("CWD = ", cwd.toString())
  }
}
