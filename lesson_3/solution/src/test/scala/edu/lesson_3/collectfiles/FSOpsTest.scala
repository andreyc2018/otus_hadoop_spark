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
    var position = 0
    readFromFile.readFully(position, byteArray, offset, blockSize)
    printf("read size = %d\n", byteArray.length)
    val str = new String(byteArray, StandardCharsets.UTF_8)
    println("str = %s".format(str))
    assert(!byteArray.isEmpty)
  }

   test("another read from file") {
     val fs_ops = new FSOps("hdfs://localhost:9000")
     val file = new Path("/stage/date=2020-12-03/part-0001.csv")
     val readFromFile: FSDataInputStream = fs_ops.fs.open(file)
     val in = new BufferedInputStream(readFromFile)
     val blockSize = 1024
     val byteArray: Array[Byte] = new Array[Byte](blockSize)
     var numBytes = 0
     do {
       numBytes = in.read(byteArray)
       val str = new String(byteArray, StandardCharsets.UTF_8)
       printf("%d: %s\n", numBytes, str)
     } while (numBytes > 0)
     in.close()
     assert(numBytes == -1)
   }

  test("read from two files and write to one") {
    val fs_ops = new FSOps("hdfs://localhost:9000")
    val file_1 = new Path("/stage/date=2020-12-03/part-0001.csv")
    val in_1 = new BufferedInputStream(fs_ops.fs.open(file_1))
    val out_1 = fs_ops.fs.create(new Path("/newdir/part-0000.csv"))

    val byteArray = new Array[Byte](1024)
    var data = ArrayBuffer[Byte]()
    var numBytes = 0
    var totalBytes = 0
    do {
      numBytes = in_1.read(byteArray)
      if (numBytes > 0) {
        val bytes = byteArray.slice(0, numBytes)
        data ++= bytes
        val str_1 = new String(data.toArray.slice(totalBytes, totalBytes+numBytes), StandardCharsets.UTF_8)
        printf("data slice: %s\n", str_1.substring(0, 50))
        totalBytes += numBytes
        val str = new String(bytes, StandardCharsets.UTF_8)
        printf("byteArray size = %d, numBytes = %d: %s\n", byteArray.length, numBytes, str.substring(0, 50))
      }
    } while (numBytes > 0)
    in_1.close()
    assert(numBytes <= 0)

    val file_2 = new Path("/stage/date=2020-12-03/part-0002.csv")
    val in_2 = new BufferedInputStream(fs_ops.fs.open(file_2))

    val str_1 = new String(data.toArray, StandardCharsets.UTF_8)
    printf("total = %d, data length = %d:\n%s\n", totalBytes, data.length, str_1.substring(0, 50))

    do {
      numBytes = in_2.read(byteArray)
      if (numBytes > 0) {
        val bytes = byteArray.slice(0, numBytes)
        val str = new String(bytes, StandardCharsets.UTF_8)
        printf("byteArray size = %d, numBytes = %d:\n%s\n", byteArray.length, numBytes, str)

        data ++= bytes
//        val str_1 = new String(data.toArray.slice(totalBytes, totalBytes+numBytes), StandardCharsets.UTF_8)
        val str_1 = new String(data.toArray, StandardCharsets.UTF_8)
        printf("data:\n%s\n", str_1)
        totalBytes += numBytes
      }
    } while (numBytes > 0)
    val str_2 = new String(data.toArray, StandardCharsets.UTF_8)
    printf("total = %d, data length = %d:\n%s\n", totalBytes, data.length, str_2)
    in_2.close()
    out_1.write(data.toArray)
    out_1.close()
    assert(numBytes == -1)
  }
}
