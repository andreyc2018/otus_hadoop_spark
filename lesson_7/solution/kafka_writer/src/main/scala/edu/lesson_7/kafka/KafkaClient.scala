package edu.lesson_7.kafka

object KafkaClient extends App {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  printf("currentDirectory \"%s\"\n", currentDirectory)

  if (args.length == 0) {
    println("Input filename is required.")
    System.exit(1)
  }

  // https://stackoverflow.com/questions/27781020/read-csv-in-scala-into-case-class-instances-with-error-handling

  val inputFile = args(0)
  private val writer: KafkaWriter = new KafkaWriter(inputFile)

}
