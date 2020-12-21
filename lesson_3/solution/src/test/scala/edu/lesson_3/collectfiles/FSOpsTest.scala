package edu.lesson_3.collectfiles

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.io.BufferedInputStream
import java.io.FileInputStream
import scala.collection.mutable.ArrayBuffer

class FSOpsTest extends AnyFunSuite with BeforeAndAfter {

  test("list directories") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val dirs = fs_ops.listDirs("/", "*")
    dirs.foreach(f => println(f.toString))
    assert(!dirs.isEmpty)
  }

  test("make directories") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val status = fs_ops.makeDirs("/test_dir/test_subdir_1/test_suddir_2")
    assert(status)
  }

  test("read from two files and write to one") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val data = fs_ops.readFile("/stage/date=2020-12-03/part-0001.csv")
    data ++= fs_ops.readFile("/stage/date=2020-12-03/part-0002.csv")
    fs_ops.writeToFile("/newdir/part-0000.csv", data.toArray.dropRight(1))
    assert(data.length == 1964)
  }
}
