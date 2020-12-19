package edu.lesson_3.collectfiles

trait FSOps {
  def listDirs(path: String, filter: String)
  def listFiles(dir: String, filter: String)
  def appendToFile(src: String, dst: String)
}

class HDFSOps extends FSOps {
  override def listDirs(path: String, filter: String): Unit = {
//    ???
  }

  override def listFiles(dir: String, filter: String): Unit = {
//    ???
  }

  override def appendToFile(src: String, dst: String): Unit = {
//    ???
  }
}