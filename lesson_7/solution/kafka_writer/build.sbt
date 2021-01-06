name := "kafka_writer"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.play" %% "play-json" % "2.9.1",
  "org.scalatest" %% "scalatest" % "latest.integration" % "test"
)

scalacOptions += "-deprecation"
