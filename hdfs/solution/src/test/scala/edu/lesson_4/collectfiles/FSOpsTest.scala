package edu.lesson_4.collectfiles

import org.scalatest.funsuite.AnyFunSuite

class FSOpsTest extends AnyFunSuite {

  test("list directories") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val dirs = fs_ops.listDirs("/", "*")
    dirs.foreach(f => println(f.toString))
    assert(!dirs.isEmpty)
  }

  test("list files") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val files = fs_ops.listFiles("/stage/date=2020-12-01", "*.csv")
    files.foreach(f => println("%s".format(f)))
    assert(!files.isEmpty)
  }

  test("make directories") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    assert(fs_ops.makeDirs("/test_dir/test_subdir_1/test_suddir_2"))
    assert(fs_ops.deleteEntry("/test_dir"))
  }

  test("read from two files and write to one") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val file_1 = "/stage/date=2020-12-03/part-0001.csv"
    val file_2= "/stage/date=2020-12-03/part-0002.csv"
    val data = fs_ops.readFile(file_1)
    val size_1 = fs_ops.fileSize(file_1)
    data ++= fs_ops.readFile(file_2)
    val size_2 = fs_ops.fileSize(file_2)
    fs_ops.writeToFile("/newdir/part-0000.csv", data.toArray.dropRight(1))
    assert(data.length == size_1 + size_2 + 2) // each readFile adds '\n' at the end of data
    assert(fs_ops.deleteEntry("/newdir"))
  }
}
