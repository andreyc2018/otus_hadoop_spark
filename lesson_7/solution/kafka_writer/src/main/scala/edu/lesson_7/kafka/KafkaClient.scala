package edu.lesson_7.kafka

import java.io.FileNotFoundException
import scala.collection.mutable.Queue

object KafkaClient extends App {

  if (args.length == 0) {
    println("Input filename is required.")
    System.exit(1)
  }

  val inputFile = args(0)
  try {
    Writer.fromFile(inputFile, "books")
    val records = Reader.readFrom("books")
    records.foreach(p => {
      println(s"Partition ${p._1}")
      printPartition(p._2)
    })

  } catch {
    case e: FileNotFoundException => { println(s"Error: ${e.getMessage}")}
    case e: Exception => { println(s"Error: ${e.getMessage}")}
  }

  private def printRecord(record: BookRecord): Unit = {
    println(s"${record.offset}: ${record.data}")
  }

  private def printPartition(partition: Queue[BookRecord]): Unit = {
    partition.foreach(r => printRecord(r))
    println()
  }

}
