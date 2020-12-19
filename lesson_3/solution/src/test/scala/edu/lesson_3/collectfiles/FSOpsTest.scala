package edu.lesson_3.collectfiles

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class FSOpsTest extends AnyFunSuite with BeforeAndAfter {
  var fs: FSOps = _

  before {
    fs = new HDFSOps
  }

  test("list directories") {
//    val dirs = fs.listDirs("/test_root", "dir_*")
//    assert(!dirs.isEmpty)
  }
}
