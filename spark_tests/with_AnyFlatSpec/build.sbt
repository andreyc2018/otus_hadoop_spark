name := "code_refactor"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "2.4.7"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val scalaTestVersion = "3.2.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

scalacOptions += "-deprecation"

