name := "with_SharedSparkSession"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.0"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val scalaTestVersion = "3.2.1"
val flinkVersion = "1.12.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests"
)

scalacOptions += "-deprecation"
