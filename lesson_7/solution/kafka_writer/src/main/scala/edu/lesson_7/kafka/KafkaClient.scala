package edu.lesson_7.kafka

object KafkaClient extends App {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  printf("currentDirectory \"%s\"\n", currentDirectory)

  if (args.length == 0) {
    println("Input filename is required.")
    System.exit(1)
  }

  val inputFile = args(0)
  Writer.fromFile(inputFile)
}
