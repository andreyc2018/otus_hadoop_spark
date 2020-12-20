package edu.lesson_3.collectfiles

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

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
}
