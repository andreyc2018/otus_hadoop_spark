package edu.lesson_3.collectfiles

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets
import java.io.BufferedInputStream
import java.io.FileInputStream

class FSOpsTest extends AnyFunSuite with BeforeAndAfter {

  test("list directories") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val dirs = fs_ops.listDirs("/", "*")
    dirs.foreach(f => println(f.toString))
    assert(!dirs.isEmpty)
  }

  test("make directory") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val status = fs_ops.makeDirs("test_dir")
    assert(status)
  }

  test("make directories") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val status = fs_ops.makeDirs("test_dir/test_subdir_1/test_suddir_2")
    assert(status)
  }

  test("read from file") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val file = new Path("/stage/date=2020-12-03/part-0001.csv")
    val readFromFile: FSDataInputStream = fs_ops.fs.open(file)
    val blockSize = 128
    val byteArray: Array[Byte] = new Array[Byte](blockSize)
    var offset = 0
    var possition = 0
    readFromFile.readFully(possition, byteArray, offset, blockSize)
    printf("read size = %d\n", byteArray.length)
    val str = new String(byteArray, StandardCharsets.UTF_8)
    println("str = %s".format(str))
    println("data = %s".format(byteArray))
    assert(!byteArray.isEmpty)
  }

  // test("another read from file") {
  //   val fs_ops = new FSOps("hdfs://localhost:9000")
  //   val file = new Path("/stage/date=2020-12-03/part-0001.csv")
  //   val readFromFile: FSDataInputStream = fs_ops.fs.open(file)
  //   val in = new BufferedInputStream(readFromFile)
  //   val blockSize = 128
  //   val byteArray: Array[Byte] = new Array[Byte](blockSize)
  //   var numBytes = in.read(byteArray)
  //   while (numBytes > 0) {
  //     // out.write(b, 0, numBytes)
  //     numBytes = in.read(byteArray)
  //   }
  //   in.close()
  //   var offset = 0
  //   var possition = 0
  //   readFromFile.readFully(possition, byteArray, offset, blockSize)
  //   val str = new String(byteArray, StandardCharsets.UTF_8)
  //   println("data = ".format(str))
  //   assert(!byteArray.isEmpty)
  // }
}
