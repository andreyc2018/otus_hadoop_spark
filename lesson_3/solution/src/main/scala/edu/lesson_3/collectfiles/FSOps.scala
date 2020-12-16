package edu.lesson_3.collectfiles

trait FSOps {
  def listDirs(path: String, filter: String)
  def listFiles(dir: String, filter: String)
  def appendFile(src: String, dst: String)
}
